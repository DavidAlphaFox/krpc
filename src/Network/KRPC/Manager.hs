-- |
--   Copyright   :  (c) Sam Truzjan 2013
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
{-# LANGUAGE TemplateHaskell        #-}
module Network.KRPC.Manager
       ( -- * Manager
         MonadKRPC (..)
       , Manager
       , newManager
       , closeManager
       , withManager
       , listen

         -- * Queries
       , query
       , Handler
       , handler
       ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Lifted (fork)
import Control.Exception hiding (Handler)
import Control.Exception.Lifted as Lifted (catch)
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.Trans.Control
import Data.BEncode as BE
import Data.ByteString.Char8 as BC
import Data.ByteString.Lazy as BL
import Data.IORef
import Data.List as L
import Data.Map as M
import Data.Monoid
import Data.Text as T
import Data.Text.Encoding as T
import Data.Tuple
import Network.KRPC.Message
import Network.KRPC.Method
import Network.Socket hiding (listen)
import Network.Socket.ByteString as BS
import System.Timeout


type KResult = Either KError KResponse

type TransactionCounter = IORef Int
type CallId             = (TransactionId, SockAddr)
type CallRes            = MVar KResult
type PendingCalls       = IORef (Map CallId CallRes)

type HandlerBody h = SockAddr -> BValue -> h (BE.Result BValue)

-- | Handler is a function which will be invoked then some /remote/
-- node querying /this/ node.
type Handler     h = (MethodName, HandlerBody h)

-- | Keep track pending queries made by /this/ node and handle queries
-- made by /remote/ nodes.
data Manager h = Manager
  { sock               :: !Socket
  , queryTimeout       :: !Int -- ^ in seconds
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

seedTransaction :: Int
seedTransaction = 0

defaultQueryTimeout :: Int
defaultQueryTimeout = 120

-- | Bind socket to the specified address. To enable query handling
-- run 'listen'.
newManager :: SockAddr       -- ^ address to listen on;
           -> [Handler h]    -- ^ handlers to run on incoming queries.
           -> IO (Manager h) -- ^ new manager.
newManager servAddr handlers = do
    sock  <- bindServ
    tref  <- newEmptyMVar
    tran  <- newIORef seedTransaction
    calls <- newIORef M.empty
    return $ Manager sock defaultQueryTimeout tref tran calls handlers
  where
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

-- | Normally you should use Control.Monad.Trans.Resource.allocate
-- function.
withManager :: SockAddr -> [Handler h] -> (Manager h -> IO a) -> IO a
withManager addr hs = bracket (newManager addr hs) closeManager

{-----------------------------------------------------------------------
--  Logging
-----------------------------------------------------------------------}

querySignature :: MethodName -> TransactionId -> SockAddr -> Text
querySignature name transaction addr = T.concat
  [  "&", T.decodeUtf8 name
  , " #", T.decodeUtf8 transaction
  , " @", T.pack (show addr)
  ]

{-----------------------------------------------------------------------
--  Client
-----------------------------------------------------------------------}

sendMessage :: MonadIO m => BEncode a => Socket -> SockAddr -> a -> m ()
sendMessage sock addr a = do
  liftIO $ sendManyTo sock (BL.toChunks (BE.encode a)) addr

genTransactionId :: TransactionCounter -> IO TransactionId
genTransactionId ref = do
  cur <- atomicModifyIORef' ref $ \ cur -> (succ cur, cur)
  return $ BC.pack (show cur)

registerQuery :: CallId -> PendingCalls -> IO CallRes
registerQuery cid ref = do
  ares <- newEmptyMVar
  atomicModifyIORef' ref $ \ m -> (M.insert cid ares m, ())
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
  KResponse {..} <- either throwIO pure res
  case fromBEncode respVals of
    Right r -> pure r
    Left  e -> throwIO $ decodeError e respId

-- | Enqueue query to the given node.
--
--  This function will throw exception if quered node respond with
--  @error@ message or timeout expires.
--
query :: forall h m a b. (MonadKRPC h m, KRPC a b) => SockAddr -> a -> m b
query addr params = do
  Manager {..} <- getManager
  tid <- liftIO $ genTransactionId transactionCounter
  let queryMethod = method :: Method a b
  let signature = querySignature (methodName queryMethod) tid addr
  $(logDebugS) "query.sending" signature

  mres <- liftIO $ do
    ares <- registerQuery (tid, addr) pendingCalls

    let q = KQuery (toBEncode params) (methodName queryMethod) tid
    sendMessage sock addr q
      `onException` unregisterQuery (tid, addr) pendingCalls

    timeout (queryTimeout * 10 ^ (6 :: Int)) $ do
      queryResponse ares

  case mres of
    Just res -> do
      $(logDebugS) "query.responded" $ signature
      return res

    Nothing -> do
      _ <- liftIO $ unregisterQuery (tid, addr) pendingCalls
      $(logWarnS) "query.not_responding" $ signature
               <> " for " <> T.pack (show queryTimeout) <> " seconds"
      throw $ timeoutExpired tid

{-----------------------------------------------------------------------
--  Handlers
-----------------------------------------------------------------------}

-- | Make handler from handler function. Any thrown exception will be
-- supressed and send over the wire back to the querying node.
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
runHandler h addr KQuery {..} = wrapper `Lifted.catch` failback
  where
    signature = querySignature queryMethod queryId addr

    wrapper = do
      $(logDebugS) "handler.quered" signature
      result <- liftHandler (h addr queryArgs)

      case result of
        Left msg -> do
          $(logDebugS) "handler.failed" $ signature <> " !" <> T.pack msg
          return $ Left  $ decodeError msg queryId

        Right a -> do
          $(logDebugS) "handler.success" signature
          return $ Right $ a `KResponse`   queryId

    failback e = do
      $(logDebugS) "handler.errored" signature
      return $ Left $ serverError e queryId

dispatchHandler :: MonadKRPC h m => KQuery -> SockAddr -> m KResult
dispatchHandler q @ KQuery {..} addr = do
  Manager {..} <- getManager
  case L.lookup queryMethod handlers of
    Nothing -> return $ Left $ unknownMethod queryMethod queryId
    Just h  -> runHandler h addr q

{-----------------------------------------------------------------------
--  Listener
-----------------------------------------------------------------------}

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

maxMsgSize :: Int
maxMsgSize = 64 * 1024

listener :: MonadKRPC h m => m ()
listener = do
  Manager {..} <- getManager
  forever $ do
    (bs, addr) <- liftIO $ BS.recvFrom sock maxMsgSize
    case BE.decode bs of
      Left  e -> liftIO $ sendMessage sock addr  $ unknownMessage e
      Right m -> handleMessage m addr

-- | Should be run before any 'query', otherwise they will never
-- succeed.
listen :: MonadKRPC h m => m ()
listen = do
  Manager {..} <- getManager
  tid <- fork $ listener
  liftIO $ putMVar listenerThread tid
