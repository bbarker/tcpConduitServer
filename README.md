# tcpConduitServer

# Generating the script

To update the script `./conduitServer.hs` to reflect the code under `app`, run `makeScript.sh`.

# Running

## Script

```
./conduitServer.hs
```

## Executable

```
stack --nix clean && stack --nix build && stack --nix exec tcpConduitServer-exe +RTS -N4
```

