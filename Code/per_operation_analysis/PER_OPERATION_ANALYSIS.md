# Per-Operation Performance Analysis

## MyBST vs MyBSTBaseline - Detailed Comparison

---

## Overall Statistics

| Operation | Avg Speedup | Min Speedup | Max Speedup | Workloads |
|-----------|-------------|-------------|-------------|-----------|
| **Insert** | 5.50x | 2.62x | 13.51x | 6 |
| **Delete** | 2.50x | 1.83x | 4.05x | 6 |
| **Contains** | 1.30x | 0.99x | 1.91x | 6 |
| **Size** | 0.92x | 0.88x | 0.96x | 4 |
| **Rank** | 0.93x | 0.88x | 0.97x | 4 |
| **Select** | 0.93x | 0.88x | 0.97x | 4 |
| **Overall** | 1.40x | 1.03x | 2.31x | 6 |

---

## Detailed Per-Workload Analysis

### ins10-del10-con26-size16-rank17-sel17

**Configuration:** 96 total threads
- Insert: 10 threads
- Delete: 10 threads
- Contains: 26 threads
- Size: 16 threads
- Rank: 17 threads
- Select: 17 threads

| Operation | MyBSTBaseline | MyBST | Speedup | Status |
|-----------|---------------|-------|---------|--------|
| Insert | 165,630 | 460,909 | 2.78x (+178.3%) | ✓ |
| Delete | 325,087 | 594,030 | 1.83x (+82.7%) | ✓ |
| Contains | 8,257,912 | 9,937,057 | 1.20x (+20.3%) | ✓ |
| Size | 14,909 | 14,360 | 0.96x (-3.7%) | ✗ |
| Rank | 15,781 | 15,364 | 0.97x (-2.6%) | ✗ |
| Select | 15,787 | 15,364 | 0.97x (-2.7%) | ✗ |
| Total | 8,795,106 | 11,037,084 | 1.25x (+25.5%) | ✓ |

---

### ins10-del10-con50-size10-rank10-sel10

**Configuration:** 100 total threads
- Insert: 10 threads
- Delete: 10 threads
- Contains: 50 threads
- Size: 10 threads
- Rank: 10 threads
- Select: 10 threads

| Operation | MyBSTBaseline | MyBST | Speedup | Status |
|-----------|---------------|-------|---------|--------|
| Insert | 170,227 | 445,467 | 2.62x (+161.7%) | ✓ |
| Delete | 327,387 | 642,669 | 1.96x (+96.3%) | ✓ |
| Contains | 15,094,293 | 14,952,846 | 0.99x (-0.9%) | ✗ |
| Size | 9,205 | 8,649 | 0.94x (-6.0%) | ✗ |
| Rank | 9,170 | 8,670 | 0.95x (-5.5%) | ✗ |
| Select | 9,175 | 8,666 | 0.94x (-5.6%) | ✗ |
| Total | 15,619,457 | 16,066,965 | 1.03x (+2.9%) | ✓ |

---

### ins10-del10-con60-size6-rank7-sel7

**Configuration:** 100 total threads
- Insert: 10 threads
- Delete: 10 threads
- Contains: 60 threads
- Size: 6 threads
- Rank: 7 threads
- Select: 7 threads

| Operation | MyBSTBaseline | MyBST | Speedup | Status |
|-----------|---------------|-------|---------|--------|
| Insert | 141,811 | 440,212 | 3.10x (+210.4%) | ✓ |
| Delete | 272,844 | 508,735 | 1.86x (+86.5%) | ✓ |
| Contains | 18,606,540 | 22,389,444 | 1.20x (+20.3%) | ✓ |
| Size | 5,602 | 5,061 | 0.90x (-9.7%) | ✗ |
| Rank | 6,511 | 5,911 | 0.91x (-9.2%) | ✗ |
| Select | 6,518 | 5,908 | 0.91x (-9.4%) | ✗ |
| Total | 19,039,826 | 23,355,272 | 1.23x (+22.7%) | ✓ |

---

### ins10-del10-con70-size3-rank3-sel4

**Configuration:** 100 total threads
- Insert: 10 threads
- Delete: 10 threads
- Contains: 70 threads
- Size: 3 threads
- Rank: 3 threads
- Select: 4 threads

| Operation | MyBSTBaseline | MyBST | Speedup | Status |
|-----------|---------------|-------|---------|--------|
| Insert | 155,440 | 574,855 | 3.70x (+269.8%) | ✓ |
| Delete | 338,190 | 740,103 | 2.19x (+118.8%) | ✓ |
| Contains | 20,403,330 | 23,704,915 | 1.16x (+16.2%) | ✓ |
| Size | 2,775 | 2,446 | 0.88x (-11.8%) | ✗ |
| Rank | 2,766 | 2,435 | 0.88x (-12.0%) | ✗ |
| Select | 3,692 | 3,241 | 0.88x (-12.2%) | ✗ |
| Total | 20,906,193 | 25,027,994 | 1.20x (+19.7%) | ✓ |

---

### ins3-del2-con95-size0-rank0-sel0

**Configuration:** 100 total threads
- Insert: 3 threads
- Delete: 2 threads
- Contains: 95 threads
- Size: 0 threads
- Rank: 0 threads
- Select: 0 threads

| Operation | MyBSTBaseline | MyBST | Speedup | Status |
|-----------|---------------|-------|---------|--------|
| Insert | 129,748 | 942,887 | 7.27x (+626.7%) | ✓ |
| Delete | 175,341 | 541,539 | 3.09x (+208.8%) | ✓ |
| Contains | 52,365,313 | 70,171,681 | 1.34x (+34.0%) | ✓ |
| Size | - | - | - | N/A |
| Rank | - | - | - | N/A |
| Select | - | - | - | N/A |
| Total | 52,670,402 | 71,656,106 | 1.36x (+36.0%) | ✓ |

---

### ins30-del20-con50-size0-rank0-sel0

**Configuration:** 100 total threads
- Insert: 30 threads
- Delete: 20 threads
- Contains: 50 threads
- Size: 0 threads
- Rank: 0 threads
- Select: 0 threads

| Operation | MyBSTBaseline | MyBST | Speedup | Status |
|-----------|---------------|-------|---------|--------|
| Insert | 266,048 | 3,594,497 | 13.51x (+1251.1%) | ✓ |
| Delete | 331,912 | 1,344,437 | 4.05x (+305.1%) | ✓ |
| Contains | 8,969,528 | 17,145,084 | 1.91x (+91.1%) | ✓ |
| Size | - | - | - | N/A |
| Rank | - | - | - | N/A |
| Select | - | - | - | N/A |
| Total | 9,567,488 | 22,084,018 | 2.31x (+130.8%) | ✓ |

---

## Key Findings

### Insert
- **Best:** ins30-del20-con50-size0-rank0-sel0 (13.51x)
- **Worst:** ins10-del10-con50-size10-rank10-sel10 (2.62x)
- **Average:** 5.50x

### Delete
- **Best:** ins30-del20-con50-size0-rank0-sel0 (4.05x)
- **Worst:** ins10-del10-con26-size16-rank17-sel17 (1.83x)
- **Average:** 2.50x

### Contains
- **Best:** ins30-del20-con50-size0-rank0-sel0 (1.91x)
- **Worst:** ins10-del10-con50-size10-rank10-sel10 (0.99x)
- **Average:** 1.30x

### Size
- **Best:** ins10-del10-con26-size16-rank17-sel17 (0.96x)
- **Worst:** ins10-del10-con70-size3-rank3-sel4 (0.88x)
- **Average:** 0.92x

### Rank
- **Best:** ins10-del10-con26-size16-rank17-sel17 (0.97x)
- **Worst:** ins10-del10-con70-size3-rank3-sel4 (0.88x)
- **Average:** 0.93x

### Select
- **Best:** ins10-del10-con26-size16-rank17-sel17 (0.97x)
- **Worst:** ins10-del10-con70-size3-rank3-sel4 (0.88x)
- **Average:** 0.93x

### Overall
- **Best:** ins30-del20-con50-size0-rank0-sel0 (2.31x)
- **Worst:** ins10-del10-con50-size10-rank10-sel10 (1.03x)
- **Average:** 1.40x
