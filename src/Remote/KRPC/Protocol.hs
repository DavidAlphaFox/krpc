-- |
--   Copyright   :  (c) Sam T. 2013
--   License     :  MIT
--   Maintainer  :  pxqr.sta@gmail.com
--   Stability   :  experimental
--   Portability :  portable
--
--   This module provides straightforward implementation of KRPC
--   protocol. In many situations 'Network.KRPC' should be prefered
--   since it gives more safe, convenient and high level api.
--
--   > See http://www.bittorrent.org/beps/bep_0005.html#krpc-protocol
--
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE FlexibleContexts, TypeSynonymInstances #-}
{-# LANGUAGE MultiParamTypeClasses, FunctionalDependencies #-}
{-# LANGUAGE DefaultSignatures #-}
module Remote.KRPC.Protocol
       (

         -- * Error
         KError(..), ErrorCode, errorCode, mkKError

         -- * Query
       , KQuery(queryMethod, queryArgs), MethodName, ParamName, kquery

         -- * Response
       , KResponse(respVals), ValName, kresponse

       , sendMessage, recvResponse

         -- * Remote
       , KRemote, KRemoteAddr, withRemote, remoteServer

         -- * Re-exports
       , encode, encoded, decode, decoded, toBEncode, fromBEncode
       ) where

import Prelude hiding (catch)
import Control.Applicative
import Control.Exception.Lifted
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Control

import Data.BEncode
import Data.ByteString as B
import Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as LB
import Data.Map as M

import Network.Socket hiding (recvFrom)
import Network.Socket.ByteString


-- | Errors used to signal that some error occurred while processing a
-- procedure call. Error may be send only from server to client but
-- not in the opposite direction.
--
--   Errors are encoded as bencoded dictionary:
--
--   { "y" : "e", "e" : [<error_code>, <human_readable_error_reason>] }
--
data KError
    -- | Some error doesn't fit in any other category.
  = GenericError { errorMessage :: ByteString }

    -- | Occur when server fail to process procedure call.
  | ServerError  { errorMessage :: ByteString }

    -- | Malformed packet, invalid arguments or bad token.
  | ProtocolError { errorMessage :: ByteString }

    -- | Occur when client trying to call method server don't know.
  | MethodUnknown { errorMessage :: ByteString }
   deriving (Show, Read, Eq, Ord)

instance BEncodable KError where
  {-# SPECIALIZE instance BEncodable KError #-}
  {-# INLINE toBEncode #-}
  toBEncode e = fromAscAssocs -- WARN: keep keys sorted
    [ "e" --> (errorCode e, errorMessage e)
    , "y" --> ("e" :: ByteString)
    ]

  {-# INLINE fromBEncode #-}
  fromBEncode (BDict d)
    | M.lookup "y" d == Just (BString "e")
    = uncurry mkKError <$> d >-- "e"

  fromBEncode _ = decodingError "KError"

type ErrorCode = Int

errorCode :: KError -> ErrorCode
errorCode (GenericError _)  = 201
errorCode (ServerError _)   = 202
errorCode (ProtocolError _) = 203
errorCode (MethodUnknown _) = 204
{-# INLINE errorCode #-}

mkKError :: ErrorCode -> ByteString -> KError
mkKError 201 = GenericError
mkKError 202 = ServerError
mkKError 203 = ProtocolError
mkKError 204 = MethodUnknown
mkKError _   = GenericError
{-# INLINE mkKError #-}

serverError :: SomeException -> KError
serverError = ServerError . BC.pack . show

-- TODO Asc everywhere


type MethodName = ByteString
type ParamName  = ByteString

-- | Query used to signal that caller want to make procedure call to
-- callee and pass arguments in. Therefore query may be only sent from
-- client to server but not in the opposite direction.
--
--   Queries are encoded as bencoded dictionary:
--
--     { "y" : "q", "q" : "<method_name>", "a" : [<arg1>, <arg2>, ...] }
--
data KQuery = KQuery {
    queryMethod :: MethodName
  , queryArgs   :: Map ParamName BEncode
  } deriving (Show, Read, Eq, Ord)

instance BEncodable KQuery where
  {-# SPECIALIZE instance BEncodable KQuery #-}
  {-# INLINE toBEncode #-}
  toBEncode (KQuery m args) = fromAscAssocs -- WARN: keep keys sorted
    [ "a" --> BDict args
    , "q" --> m
    , "y" --> ("q" :: ByteString)
    ]

  {-# INLINE fromBEncode #-}
  fromBEncode (BDict d)
    | M.lookup "y" d == Just (BString "q") =
      KQuery <$> d >-- "q"
             <*> d >-- "a"

  fromBEncode _ = decodingError "KQuery"

kquery :: MethodName -> [(ParamName, BEncode)] -> KQuery
kquery name args = KQuery name (M.fromList args)
{-# INLINE kquery #-}




type ValName = ByteString

-- | KResponse used to signal that callee successufully process a
-- procedure call and to return values from procedure. KResponse should
-- not be sent if error occurred during RPC. Thus KResponse may be only
-- sent from server to client.
--
--   Responses are encoded as bencoded dictionary:
--
--     { "y" : "r", "r" : [<val1>, <val2>, ...] }
--
newtype KResponse = KResponse {
    respVals :: Map ValName BEncode
  } deriving (Show, Read, Eq, Ord)

instance BEncodable KResponse where
  {-# SPECIALIZE instance BEncodable KResponse #-}
  {-# INLINE toBEncode #-}
  toBEncode (KResponse vals) = fromAscAssocs  -- WARN: keep keys sorted
    [ "r" --> vals
    , "y" --> ("r" :: ByteString)
    ]

  {-# INLINE fromBEncode #-}
  fromBEncode (BDict d)
    | M.lookup "y" d == Just (BString "r") =
      KResponse <$> d >-- "r"

  fromBEncode _ = decodingError "KDict"


kresponse :: [(ValName, BEncode)] -> KResponse
kresponse = KResponse . M.fromList
{-# INLINE kresponse #-}



type KRemoteAddr = (HostAddress, PortNumber)

type KRemote = Socket

withRemote :: (MonadBaseControl IO m, MonadIO m) => (KRemote -> m a) -> m a
withRemote = bracket (liftIO (socket AF_INET Datagram defaultProtocol))
                     (liftIO .  sClose)
{-# SPECIALIZE withRemote :: (KRemote -> IO a) -> IO a #-}


maxMsgSize :: Int
{-# INLINE maxMsgSize #-}
-- release
--maxMsgSize = 512 -- size of payload of one udp packet
-- bench
maxMsgSize = 64 * 1024 -- max udp size


-- TODO eliminate toStrict
sendMessage :: BEncodable msg => msg -> KRemoteAddr -> KRemote -> IO ()
sendMessage msg (host, port) sock =
  sendAllTo sock (LB.toStrict (encoded msg)) (SockAddrInet port host)
{-# INLINE sendMessage #-}
{-# SPECIALIZE sendMessage :: BEncode -> KRemoteAddr -> KRemote -> IO ()  #-}

recvResponse :: KRemote -> IO (Either KError KResponse)
recvResponse sock = do
  (raw, _) <- recvFrom sock maxMsgSize
  return $ case decoded raw of
    Right resp -> Right resp
    Left decE -> Left $ case decoded raw of
      Right kerror -> kerror
      _ -> ProtocolError (BC.pack decE)

-- | Run server using a given port. Method invocation should be done manually.
remoteServer :: (MonadBaseControl IO remote, MonadIO remote)
             => PortNumber -- ^ Port number to listen.
             -> (KRemoteAddr -> KQuery -> remote (Either KError KResponse))
             -- ^ Handler.
             -> remote ()
remoteServer servport action = bracket (liftIO bind) (liftIO . sClose) loop
  where
    bind = do
     sock <- socket AF_INET Datagram defaultProtocol
     bindSocket sock (SockAddrInet servport iNADDR_ANY)
     return sock

    loop sock = forever $ do
      (bs, addr) <- liftIO $ recvFrom sock maxMsgSize
      case addr of
        SockAddrInet port host -> do
           let kaddr = (host, port)
           reply <- handleMsg bs kaddr
           liftIO $ sendMessage reply kaddr sock
        _ -> return ()

      where
        handleMsg bs addr = case decoded bs of
          Right query -> (either toBEncode toBEncode <$> action addr query)
                        `catch` (return . toBEncode . serverError)
          Left decodeE   -> return $ toBEncode (ProtocolError (BC.pack decodeE))
