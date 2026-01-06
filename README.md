# HandshakeBST: Optimized Fast Path with Forwarding Pointers

## Project Overview
This project implements and evaluates **HandshakeBST**, a novel concurrent Binary Search Tree designed to optimize performance in workloads dominated by standard updates (insert, delete) while still supporting powerful aggregate queries (size, rank, select).

The core innovation is the application of the **Handshake synchronization technique**, which allows the data structure to operate in two distinct modes:
1.  **Fast Path**: A lightweight mode for updates when no queries are active. It avoids maintaining expensive metadata (Version trees) and instead uses **Forwarding Pointers** to handle structural changes lazily.
2.  **Slow Path**: A consistent mode entered only when an aggregate query is requested. It uses a cooperative handshake mechanism to ensure all threads agree on a consistent view of the tree.

## Code to Algorithm Mapping
The following Java files in `src/main/java/bst/` correspond to the algorithms discussed in the report:

*   **`MyBSTnext.java`** $\rightarrow$ **HandshakeBST**
    *   This is the primary contribution of this project. It implements the handshake protocol, the fast path with forwarding pointers, chain compression, and the slow path for aggregate queries.
*   **`MyBSTBaseline.java`** $\rightarrow$ **LockFreeBST**
    *   This represents the state-of-the-art baseline (Lock-Free Augmented BST by Fatourou et al.). It maintains full metadata on every update, serving as the comparison point for overhead analysis.

## Testing
To ensure the correctness of the complex concurrency mechanisms (especially the interaction between Fast and Slow paths), specific test suites were developed and are located in `src/test/java/bst/`:

## Benchmarking and External Code
The performance evaluation presented in the report was conducted using a robust benchmarking framework on an **EPFL server with 96 hardware threads**.

I also included my own custom benchmarking code in the **`src/main/java/bench/`** directory. This folder contains specific performance tests (such as `ComprehensiveBenchmark.java` and `FairComparison.java`) that I implemented to evaluate the algorithms directly on the server, allowing for fine-tuned performance analysis separate from the main comparison framework.

**Note on the code of the benchmark used for the report :**
To respect the original authors' work and avoid copyright issues, the benchmarking infrastructure and the standard **Non-Augmented BST** implementation are **not included** in this repository.

This project is a continuation of the work presented in:
> *Handshake: A synchronization technique for concurrent aggregate queries* ([arXiv:2506.16350](https://doi.org/10.48550/arXiv.2506.16350))

To view the benchmarking harness and the `BST` implementation used for comparison, please refer to the original repository:
*   **Original Repository**: [ConcurrentSizeMethods on GitHub](https://github.com/henkassharir/ConcurrentSizeMethods)

The `Benchmark_results` folder in this repository contains the raw results and data collected from the experiments run on the EPFL server, which were used to generate the graphs and analysis for the final report.
