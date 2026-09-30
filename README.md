# MIT: 6.5840: Distributed Systems - Labs

This repo contains my solution to openly available [MIT labs](https://pdos.csail.mit.edu/6.824/index.html) from their course "Distributed Systems".

## Completed labs

- Lab 1: :white_check_mark:
- Lab 2: :x:
- Lab 3: :x:
- Lab 4: :x:
- Lab 5: :x:

**Note: this repo is a work in progress, as I plan to finish all 5 labs.**

## Verification

### Lab 1 - Distributed MapReduce

1. Clone the repo \
`git clone https://github.com/kuros95/MIT-goLabs-2026`

2. Switch to `src/main` and build the wordcount (wc) plugin \
`cd MIT-goLabs-2026/src/main && go build -buildmode=plugin -o ../mrapps/wc.so ../mrapps/wc.go` \
*Note: if you modify worker.go, you will have to rebuild the plugin*

3. Run cooridinator \
`go run mrcoordinator.go /tmp/my.sock pg-being_ernest.txt pg-dorian_gray.txt` \
Syntax: `mrcoordinator.go sockname inputfiles...`

4. Run worker in a separate terminal \
`go run mrworker.go ../mrapps/wc.so /tmp/my.sock` \
Syntax: `worker.go plugin sockname`

5. Observe the output file `mr-out-0` in `src/main`

The solution has been tested on up to 4 workers, each time resulting in the same output.
