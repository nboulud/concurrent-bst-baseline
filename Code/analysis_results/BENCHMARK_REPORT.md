# 📊 Rapport d'Analyse - Benchmark MyBST

**Date:** 2025-11-09 18:59

**Total d'essais analysés:** 72

## Algorithmes testés

- **BST**: 12 essais
- **HandshakesBST**: 12 essais
- **MyBST**: 12 essais
- **MyBSTBaseline**: 12 essais
- **OptimisticSizeBST**: 12 essais
- **SizeBST**: 12 essais

## 🆚 MyBST vs MyBSTBaseline - Résultats


### Workload: `30i-20d-50f`

| Threads | MyBST (ops/s) | Baseline (ops/s) | Speedup | Amélioration |
|--------:|---------------:|-----------------:|--------:|-------------:|
| 1 | 2,143,113 | 14,602,380 | 0.147x | 🔴 -85.3% |
| 4 | 644,632 | 14,051,334 | 0.046x | 🔴 -95.4% |
| 8 | 892,648 | 11,165,327 | 0.080x | 🔴 -92.0% |
| 16 | 994,502 | 7,756,230 | 0.128x | 🔴 -87.2% |
| 32 | 1,135,265 | 5,532,364 | 0.205x | 🔴 -79.5% |
| 64 | 1,304,076 | 2,926,873 | 0.446x | 🔴 -55.4% |

### Workload: `3i-2d-95f`

| Threads | MyBST (ops/s) | Baseline (ops/s) | Speedup | Amélioration |
|--------:|---------------:|-----------------:|--------:|-------------:|
| 1 | 2,139,677 | 14,597,350 | 0.147x | 🔴 -85.3% |
| 4 | 2,460,777 | 15,682,244 | 0.157x | 🔴 -84.3% |
| 8 | 2,617,442 | 15,303,294 | 0.171x | 🔴 -82.9% |
| 16 | 4,385,033 | 16,022,248 | 0.274x | 🔴 -72.6% |
| 32 | 6,528,758 | 17,179,137 | 0.380x | 🔴 -62.0% |
| 64 | 9,391,555 | 17,298,881 | 0.543x | 🔴 -45.7% |

## 📈 Graphiques

Les graphiques suivants ont été générés:

1. **Throughput vs Threads** (par workload)
2. **Speedup Analysis** (MyBST / MyBSTBaseline)


## 🔍 Observations clés

- **Speedup moyen**: 0.227x
- **Speedup maximal**: 0.543x (64 threads, workload `3i-2d-95f`)
- MyBST **sous-performe** (>5%) dans **12** configurations
