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
`go run mrcoordinator.go /tmp/my.sock pg-being_ernest.txt pg-dorian_gray.txt pg-frankenstein.txt pg-grimm.txt pg-huckleberry_finn.txt pg-metamorphosis.txt pg-sherlock_holmes.txt pg-tom_sawyer.txt` \
Syntax: `mrcoordinator.go sockname inputfiles...`

4. Run worker in a separate terminal \
`go run mrworker.go ../mrapps/wc.so /tmp/my.sock` \
Syntax: `worker.go plugin sockname`

5. Observe the output in files `mr-out-X` in `src/main`

The solution has been tested on up to 4 workers, each time resulting in the same output.

Or you can trust the test output:

```
kuros@ubuntu-machine:~/go-labs-MIT/src$ rm -rf ./mr/mr-tmp-* && make mr
go build -race -o main/mrsequential main/mrsequential.go
go build -race -o main/mrcoordinator main/mrcoordinator.go
go build -race -o main/mrworker main/mrworker.go&
(cd mrapps && go build -race -buildmode=plugin wc.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin indexer.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin mtiming.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin rtiming.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin jobcount.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin early_exit.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin crash.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin nocrash.go) || exit 1
cd mr; go test -v -race 
=== RUN   TestWc
--- PASS: TestWc (9.18s)
=== RUN   TestIndexer
--- PASS: TestIndexer (5.96s)
=== RUN   TestMapParallel
--- PASS: TestMapParallel (8.04s)
=== RUN   TestReduceParallel
--- PASS: TestReduceParallel (11.05s)
=== RUN   TestJobCount
--- PASS: TestJobCount (12.05s)
=== RUN   TestEarlyExit
--- PASS: TestEarlyExit (4.08s)
=== RUN   TestCrashWorker
--- PASS: TestCrashWorker (44.18s)
PASS
ok      6.5840/mr       95.564s
```
