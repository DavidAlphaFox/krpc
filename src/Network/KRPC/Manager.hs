-- |
--   Copyright   :  (c) Sam Truzjan 2013, 2014
--   License     :  BSD3
--   Maintainer  :  pxqr.sta@gmail.com
--   Stability   :  experimental
--   Portability :  portable
--
--   Normally, you don't need to import this module.
--
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE DeriveDataTypeable     #-}
{-# LANGUAGE TemplateHaskell        #-}
module Network.KRPC.Manager
       ( -- * Manager
         MonadKRPC (..)
       , Options (..)
       , Manager
       , newManager
       , closeManager
       , withManager
       , isActive
       , listen

         -- * Queries
       , QueryFailure (..)
       , query
       , getQueryCount

         -- * Handlers
       , HandlerFailure (..)
       , Handler
       , handler
       ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Lifted (fork)
import Control.Exception hiding (Handler)
import qualified Control.Exception.Lifted as E (Handler (..))
import Control.Exception.Lifted as Lifted (catches, finally)
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.Trans.Control
import Data.BEncode as BE
import Data.ByteString as BS
import Data.ByteString.Char8 as BC
import Data.ByteString.Lazy as BL
import Data.Default.Class
import Data.IORef
import Data.List as L
import Data.Map as M
import Data.Monoid
import Data.Text as T
import Data.Text.Encoding as T
import Data.Tuple
import Data.Typeable
import Network.KRPC.Message
import Network.KRPC.Method
import Network.Socket hiding (listen)
import Network.Socket.ByteString as BS
import System.IO.Error
import System.Timeout


{-----------------------------------------------------------------------
--  Options
-----------------------------------------------------------------------}

-- | RPC manager options.
data Options = Options
  { -- | Initial 'TransactionId' incremented with each 'query';
    optSeedTransaction :: {-# UNPACK #-} !Int

    -- | Time to wait for response from remote node, in seconds.
  , optQueryTimeout    :: {-# UNPACK #-} !Int

    -- | Maximum number of bytes to receive.
  , optMaxMsgSize      :: {-# UNPACK #-} !Int
  } deriving (Show, Eq)

defaultSeedTransaction :: Int
defaultSeedTransaction = 0

defaultQueryTimeout :: Int
defaultQueryTimeout = 120

defaultMaxMsgSize :: Int
defaultMaxMsgSize = 64 * 1024

-- | Permissive defaults.
instance Default Options where
  def = Options
    { optSeedTransaction = defaultSeedTransaction
    , optQueryTimeout    = defaultQueryTimeout
    , optMaxMsgSize      = defaultMaxMsgSize
    }

validateOptions :: Options -> IO ()
validateOptions Options {..}
  | optQueryTimeout < 1
  = throwIO (userError "krpc: non-positive query timeout")
  | optMaxMsgSize   < 1
  = throwIO (userError "krpc: non-positive buffer size")
  |       otherwise     = return ()

{-----------------------------------------------------------------------
--  Options
-----------------------------------------------------------------------}

type KResult = Either KError KResponse

type TransactionCounter = IORef Int
type CallId             = (TransactionId, SockAddr)
type CallRes            = MVar KResult
-- PendingCalls 是个IORef，里面放着一个Map，存放CallID和返回值
type PendingCalls       = IORef (Map CallId CallRes)

type HandlerBody h = SockAddr -> BValue -> h (BE.Result BValue)

-- | Handler is a function which will be invoked then some /remote/
-- node querying /this/ node.
type Handler     h = (MethodName, HandlerBody h)

-- | Keep track pending queries made by /this/ node and handle queries
-- made by /remote/ nodes.
data Manager h = Manager
  { sock               :: !Socket
  , options            :: !Options
  , listenerThread     :: !(MVar ThreadId)
  , transactionCounter :: {-# UNPACK #-} !TransactionCounter
  , pendingCalls       :: {-# UNPACK #-} !PendingCalls
  , handlers           :: [Handler h]
  }

-- | A monad which can perform or handle queries.
class (MonadBaseControl IO m, MonadLogger m, MonadIO m)
    => MonadKRPC h m | m -> h where

  -- | Ask for manager.
  getManager :: m (Manager h)

  default getManager :: MonadReader (Manager h) m => m (Manager h)
  getManager = ask

  -- | Can be used to add logging for instance.
  liftHandler :: h a -> m a

  default liftHandler :: m a -> m a
  liftHandler = id

instance (MonadBaseControl IO h, MonadLogger h, MonadIO h)
      => MonadKRPC h (ReaderT (Manager h) h) where

  liftHandler = lift

sockAddrFamily :: SockAddr -> Family
sockAddrFamily (SockAddrInet  _ _    ) = AF_INET
sockAddrFamily (SockAddrInet6 _ _ _ _) = AF_INET6
sockAddrFamily (SockAddrUnix  _      ) = AF_UNIX

-- | Bind socket to the specified address. To enable query handling
-- run 'listen'.
newManager :: Options        -- ^ various protocol options;
           -> SockAddr       -- ^ address to listen on;
           -> [Handler h]    -- ^ handlers to run on incoming queries.
           -> IO (Manager h) -- ^ new rpc manager.
newManager opts @ Options {..} servAddr handlers = do
    validateOptions opts
    sock  <- bindServ
    tref  <- newEmptyMVar
    tran  <- newIORef optSeedTransaction
    calls <- newIORef M.empty
    return $ Manager sock opts tref tran calls handlers
  where
    --  创建udp socket
    bindServ = do
      let family = sockAddrFamily servAddr
      sock <- socket family Datagram defaultProtocol
      when (family == AF_INET6) $ do
        setSocketOption sock IPv6Only 0
      bindSocket sock servAddr
      return sock

-- | Unblock all pending calls and close socket.
closeManager :: Manager m -> IO ()
closeManager Manager {..} = do
  maybe (return ()) killThread =<< tryTakeMVar listenerThread
  -- TODO unblock calls
  close sock

-- | Check if the manager is still active. Manager becomes active
-- until 'closeManager' called.
isActive :: Manager m -> IO Bool
isActive Manager {..} = liftIO $ isBound sock
{-# INLINE isActive #-}

-- | Normally you should use Control.Monad.Trans.Resource.allocate
-- function.
withManager :: Options -> SockAddr -> [Handler h]
            -> (Manager h -> IO a) -> IO a
withManager opts addr hs = bracket (newManager opts addr hs) closeManager

{-----------------------------------------------------------------------
--  Logging
-----------------------------------------------------------------------}

-- TODO prettify log messages
querySignature :: MethodName -> TransactionId -> SockAddr -> Text
querySignature name transaction addr = T.concat
  [  "&", T.decodeUtf8 name
  , " #", T.decodeUtf8 transaction
  , " @", T.pack (show addr)
  ]

{-----------------------------------------------------------------------
--  Client
-----------------------------------------------------------------------}
-- we don't need to know about TransactionId while performing query,
-- so we introduce QueryFailure exceptions

-- | Used to signal 'query' errors.
data QueryFailure
  = SendFailed                  -- ^ unable to send query;
  | QueryFailed  ErrorCode Text -- ^ remote node return error;
  | TimeoutExpired              -- ^ remote node not responding.
  deriving (Show, Eq, Typeable)

instance Exception QueryFailure

sendMessage :: MonadIO m => BEncode a => Socket -> SockAddr -> a -> m ()
sendMessage sock addr a = do
  -- UDP发送一定量的数据
  liftIO $ sendManyTo sock (BL.toChunks (BE.encode a)) addr

genTransactionId :: TransactionCounter -> IO TransactionId
genTransactionId ref = do
  cur <- atomicModifyIORef' ref $ \ cur -> (succ cur, cur)
  return $ BC.pack (show cur)

-- | How many times 'query' call have been performed.
getQueryCount :: MonadKRPC h m => m Int
getQueryCount = do
  Manager {..} <- getManager
  curTrans <- liftIO $ readIORef transactionCounter
  return $ curTrans - optSeedTransaction options

registerQuery :: CallId -> PendingCalls -> IO CallRes
registerQuery cid ref = do
  -- 创建一个空的MVar
  ares <- newEmptyMVar
  -- 修改IORef的数据
  atomicModifyIORef' ref $ \ m -> (M.insert cid ares m, ())
  -- 反回这个Mvar给调用者
  return ares

-- simultaneous M.lookup and M.delete guarantees that we never get two
-- or more responses to the same query
unregisterQuery :: CallId -> PendingCalls -> IO (Maybe CallRes)
unregisterQuery cid ref = do
  atomicModifyIORef' ref $ swap .
    M.updateLookupWithKey (const (const Nothing)) cid

queryResponse :: BEncode a => CallRes -> IO a
queryResponse ares = do
  res <- readMVar ares
  case res of
    -- 出错，直接丢出 QueryFailed 异常
    Left  (KError   c m _) -> throwIO $ QueryFailed c (T.decodeUtf8 m)
    -- 得到结果，进行解析
    Right (KResponse {..}) ->
      case fromBEncode respVals of
        Right r -> pure r
        Left  e -> throwIO $ QueryFailed ProtocolError (T.pack e)

-- (sendmsg EINVAL)
sendQuery :: BEncode a => Socket -> SockAddr -> a -> IO ()
sendQuery sock addr q = handle sockError $ sendMessage sock addr q
  -- 发送消息并捕获异常
  where
    sockError :: IOError -> IO ()
    sockError _ = throwIO SendFailed

-- | Enqueue query to the given node.
--
--  This function should throw 'QueryFailure' exception if quered node
--  respond with @error@ message or the query timeout expires.
--
query :: forall h m a b. (MonadKRPC h m, KRPC a b) => SockAddr -> a -> m b
query addr params = do
  -- 得到Manager
  Manager {..} <- getManager
  -- 生成查询事务ID
  tid <- liftIO $ genTransactionId transactionCounter
  -- 获取查询方法
  let queryMethod = method :: Method a b
  -- 生成查询签名
  let signature = querySignature (methodName queryMethod) tid addr
  $(logDebugS) "query.sending" signature

  mres <- liftIO $ do
    -- 注册查询
    ares <- registerQuery (tid, addr) pendingCalls
    -- 生成查询的二进制包
    let q = KQuery (toBEncode params) (methodName queryMethod) tid
    -- 发送查询，并在异常的时候注销相关操作
    sendQuery sock addr q
      `onException` unregisterQuery (tid, addr) pendingCalls
    -- 增加超时处理
    timeout (optQueryTimeout options * 10 ^ (6 :: Int)) $ do
      queryResponse ares

  case mres of
    Just res -> do
      $(logDebugS) "query.responded" $ signature
      return res

    Nothing -> do
      _ <- liftIO $ unregisterQuery (tid, addr) pendingCalls
      $(logWarnS) "query.not_responding" $ signature <> " for " <>
             T.pack (show (optQueryTimeout options)) <> " seconds"
      throw $ TimeoutExpired

{-----------------------------------------------------------------------
--  Handlers
-----------------------------------------------------------------------}
-- we already throw:
--
--   * ErrorCode(MethodUnknown) in the 'dispatchHandler';
--
--   * ErrorCode(ServerError) in the 'runHandler';
--
--   * ErrorCode(GenericError) in the 'runHandler' (those can be
--   async exception too)
--
--  so HandlerFailure should cover *only* 'ProtocolError's.

-- | Used to signal protocol errors.
data HandlerFailure
  = BadAddress            -- ^ for e.g.: node calls herself;
  | InvalidParameter Text -- ^ for e.g.: bad session token.
    deriving (Show, Eq, Typeable)

instance Exception HandlerFailure

prettyHF :: HandlerFailure -> BS.ByteString
prettyHF  BadAddress               = T.encodeUtf8 "bad address"
prettyHF (InvalidParameter reason) = T.encodeUtf8 $
  "invalid parameter: " <> reason

prettyQF :: QueryFailure -> BS.ByteString
prettyQF e = T.encodeUtf8 $ "handler fail while performing query: "
  <> T.pack (show e)

-- | Make handler from handler function. Any thrown exception will be
-- supressed and send over the wire back to the querying node.
--
--   If the handler make some 'query' normally it /should/ handle
--   corresponding 'QueryFailure's.
--
handler :: forall h a b. (KRPC a b, Monad h)
        => (SockAddr -> a -> h b) -> Handler h
handler body = (name, wrapper)
  where
    Method name = method :: Method a b
    wrapper addr args =
      case fromBEncode args of
        Left  e -> return $ Left e
        Right a -> do
          r <- body addr a
          return $ Right $ toBEncode r

runHandler :: MonadKRPC h m
           => HandlerBody h -> SockAddr -> KQuery -> m KResult
runHandler h addr KQuery {..} = Lifted.catches wrapper failbacks
  where
    signature = querySignature queryMethod queryId addr

    wrapper = do
      $(logDebugS) "handler.quered" signature
      result <- liftHandler (h addr queryArgs)

      case result of
        Left msg -> do
          $(logDebugS) "handler.bad_query" $ signature <> " !" <> T.pack msg
          return $ Left $ KError ProtocolError (BC.pack msg) queryId

        Right a -> do
          $(logDebugS) "handler.success" signature
          return $ Right $ KResponse a queryId

    failbacks =
      [ E.Handler $ \ (e :: HandlerFailure) -> do
          $(logDebugS) "handler.failed" signature
          return $ Left $ KError ProtocolError (prettyHF e) queryId

        -- may happen if handler makes query and fail
      , E.Handler $ \ (e :: QueryFailure) -> do
          return $ Left $ KError ServerError (prettyQF e) queryId

        -- since handler thread exit after sendMessage we can safely
        -- suppress async exception here
      , E.Handler $ \ (e :: SomeException) -> do
          return $ Left $ KError GenericError (BC.pack (show e)) queryId
      ]

dispatchHandler :: MonadKRPC h m => KQuery -> SockAddr -> m KResult
dispatchHandler q @ KQuery {..} addr = do
  Manager {..} <- getManager
  case L.lookup queryMethod handlers of
    Nothing -> return $ Left $ KError MethodUnknown queryMethod queryId
    Just h  -> runHandler h addr q

{-----------------------------------------------------------------------
--  Listener
-----------------------------------------------------------------------}

-- TODO bound amount of parallel handler *threads*:
--
-- peer A flooding with find_node
-- peer B trying to ping peer C
-- peer B fork too many threads
-- ... space leak
--
handleQuery :: MonadKRPC h m => KQuery -> SockAddr -> m ()
handleQuery q addr = void $ fork $ do
  Manager {..} <- getManager
  res <- dispatchHandler q addr
  sendMessage sock addr $ either toBEncode toBEncode res

handleResponse :: MonadKRPC h m => KResult -> SockAddr -> m ()
handleResponse result addr = do
  Manager {..} <- getManager
  liftIO $ do
    let resultId = either errorId respId  result
    mcall <- unregisterQuery (resultId, addr) pendingCalls
    case mcall of
      Nothing   -> return ()
      Just ares -> putMVar ares result

handleMessage :: MonadKRPC h m => KMessage -> SockAddr -> m ()
handleMessage (Q q) = handleQuery    q
handleMessage (R r) = handleResponse (Right r)
handleMessage (E e) = handleResponse (Left  e)

listener :: MonadKRPC h m => m ()
listener = do
  Manager {..} <- getManager
  forever $ do
    (bs, addr) <- liftIO $ do
      handle exceptions $ BS.recvFrom sock (optMaxMsgSize options)

    case BE.decode bs of
      -- TODO ignore unknown messages at all?
      Left  e -> liftIO $ sendMessage sock addr $ unknownMessage e
      Right m -> handleMessage m addr
  where
    exceptions :: IOError -> IO (BS.ByteString, SockAddr)
    exceptions e
        -- packets with empty payload may trigger eof exception
      | isEOFError e = return  ("", SockAddrInet 0 0)
      |   otherwise  = throwIO e

-- | Should be run before any 'query', otherwise they will never
-- succeed.
listen :: MonadKRPC h m => m ()
listen = do
  Manager {..} <- getManager
  tid <- fork $ do
    listener `Lifted.finally`
      liftIO (takeMVar listenerThread)
  liftIO $ putMVar listenerThread tid
