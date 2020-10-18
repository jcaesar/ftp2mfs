# HyperProv

A tunable recursive provider go-ipfs client. Recursively enumerates files and folders and reprovides their roots to the DHT.

## Necessity
IPFS finds objects through its DHT. This DHT is forgetful, and its entries disappear over time.
Thus, a node that wants its content to be found must write its existence to the DHT regularly.
The problem: there is a limit to the rate one node can write to the DHT.
Thus, if the amount of of `NumObjects` in `ipfs repo stat` is large (I don't know the exact number. 1e6 is too much, 1e5 seems fine on some of my nodes.), not all content can be found directly.

Better and "official" explanation: https://github.com/ipfs/notes/issues/296#issuecomment-707912467

## Usage
To use it, you should set
```bash
ipfs config Reprovider.Strategy roots
```
and then either launch `hyperprov` or `cargo run --` with the IPNS or MFS paths you want to be provided.
