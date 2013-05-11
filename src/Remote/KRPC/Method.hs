-- |
--   Copyright   :  (c) Sam T. 2013
--   License     :  MIT
--   Maintainer  :  pxqr.sta@gmail.com
--   Stability   :  experimental
--   Portability :  portable
--
{-# LANGUAGE OverloadedStrings #-}
module Remote.KRPC.Method
       ( Method(methodName, methodParams, methodVals)
       , methodQueryScheme, methodRespScheme

         -- * Construction
       , method

         -- * Predefined methods
       , idM, composeM
       ) where

import Prelude hiding ((.), id)
import Control.Applicative
import Control.Category
import Control.Monad
import Data.ByteString as B
import Data.List as L
import Data.Set as S

import Remote.KRPC.Protocol


-- | The
--
--   * argument: type of method parameter
--
--   * remote: A monad used by server-side.
--
--   * result: type of return value of the method.
--
data Method param result = Method {
    -- | Name used in query and
    methodName   :: [MethodName]

    -- | Description of each parameter in /right to left/ order.
  , methodParams :: [ParamName]

    -- | Description of each return value in /right to left/ order.
  , methodVals   :: [ValName]
  }

instance Category Method where
  {-# SPECIALIZE instance Category Method #-}
  id  = idM
  {-# INLINE id #-}

  (.) = composeM
  {-# INLINE (.) #-}

methodQueryScheme :: Method a b -> KQueryScheme
methodQueryScheme = KQueryScheme <$> B.intercalate "." . methodName
                                 <*> S.fromList . methodParams
{-# INLINE methodQueryScheme #-}


methodRespScheme :: Method a b -> KResponseScheme
methodRespScheme = KResponseScheme . S.fromList . methodVals
{-# INLINE methodRespScheme #-}

-- TODO ppMethod

-- | Remote identity function. Could be used for echo servers for example.
--
--   idM = method "id" ["x"] ["y"] return
--
idM :: Method a a
idM = method "id" ["x"] ["y"]
{-# INLINE idM #-}

-- | Pipelining of two or more methods.
--
--   NOTE: composed methods will work only with this implementation of
--   KRPC, so both server and client should use this implementation,
--   otherwise you more likely get the 'ProtocolError'.
--
composeM :: Method b c -> Method a b -> Method a c
composeM g h = Method (methodName g ++ methodName h)
                      (methodParams h)
                      (methodVals g)
{-# INLINE composeM #-}


method :: MethodName -> [ParamName] -> [ValName] -> Method param result
method name = Method [name]
{-# INLINE method #-}