#!/usr/bin/env bash
cp scriptHeader.hs conduitServer.hs
cat app/Main.hs >> conduitServer.hs
chmod a+x conduitServer.hs

