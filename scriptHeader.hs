#!/usr/bin/env stack
{- stack script --nix --resolver lts-14.27
  --nix-packages zlib
  --no-nix-pure
  --package bytestring
  --package classy-prelude
  --package conduit
  --package exceptions
  --package mtl
  --package network
  --package network-simple
  --package stm
  --package stm-conduit
  --package text
  --package unliftio
  --ghc-options -Wall
-}
