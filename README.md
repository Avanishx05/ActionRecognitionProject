# Action Recognition: Temporal Generalization Study

**Status:** Data pipeline and EDA done across 4 datasets. Experiment designed but not implemented yet.

---

## What this is about

Pegeot et al. (WACV 2025) showed that temporal shift hurts image classification — even strong models like DinoV2 drop in accuracy when trained on older data and tested on newer data. The gap grows the further apart the training and test periods are.

This project asks the same question for instructional video.

Instructional video feels like a particularly interesting case. Filming style, production quality, how people demonstrate tasks on camera — all of that has changed a lot over the past decade. A model trained on 2010 cooking videos probably struggles more on 2018 cooking videos than standard benchmarks would suggest, because those benchmarks shuffle data randomly and never surface the temporal gap.

The idea is to test this properly with a temporal train/test split across four large-scale datasets and see how much it actually costs five SOTA action recognition models.

---

## Models

| Model | Type | Notes |
|---|---|---|
| [InternVideo2](https://arxiv.org/abs/2403.15377) | Video-Language Transformer | Current SOTA on several benchmarks |
| [VideoSwin](https://arxiv.org/abs/2106.13230) | Hierarchical Transformer | Swin backbone adapted for video |
| [ViViT](https://arxiv.org/abs/2103.15691) | Pure Transformer | Factorized spatiotemporal attention |
| [I3D](https://arxiv.org/abs/1705.07750) | 3D CNN | Inflated Inception, still a solid baseline |
| [R3D](https://arxiv.org/abs/1711.11248) | 3D CNN | ResNet-based, lighter weight |

---

## Datasets

The key requirement was temporal metadata — upload year per video. Without that, a temporal split isn't possible. Ended up with four datasets that had it:

| Dataset | Domain | Year Range | Scale |
|---|---|---|---|
| [YouCook2](http://youcook2.eecs.umich.edu/) | Cooking | 2006–2016 | ~2,000 videos |
| [HowTo100M](https://www.di.ens.fr/willow/research/howto100m/) | General instructional | 2005–2019 | ~1M videos |
| [CrossTask](https://github.com/DmZhukov/CrossTask) | Procedural tasks | 2006–2018 | ~4,700 videos |
| [COIN](https://coin-dataset.github.io/) | Comprehensive instructional | 2006–2018 | ~11,000 videos |

Raw video isn't in this repo. Collection and organization scripts are.

---

## Experiment design

- **Train** on videos uploaded before cutoff year *T*
- **Test** on videos uploaded after year *T*
- **Baseline** — same data, random split

Run all five models under both conditions. Main thing to measure is how much performance drops going from random to temporal split. Secondary analysis breaks it down by action category — Pegeot et al. found human-made object categories get hit harder than natural ones, curious if something similar shows up here.

---

## Where things stand

- [x] Dataset collection pipeline — 4 datasets with upload year metadata extracted
- [x] Temporal split logic and stratification
- [x] EDA — video distributions by year across all four datasets
- [ ] Model training runs
- [ ] Temporal generalization experiments
- [ ] Results and write-up

---

## What the EDA showed

All four datasets are heavily skewed toward more recent years, which matters for how you pick the cutoff.

**YouCook2:** Peaks at 460 videos in 2014, very sparse before 2010. A 2013 cutoff works reasonably well.

**HowTo100M:** Grows steadily from near zero in 2005 to ~181K in 2017. Big enough that the split is flexible — the anchor dataset for this reason.

**CrossTask:** Ramps up from 2011, peaks at 730 in 2016, drops sharply after. The 2015–2016 spike is worth looking into — not clear if it's a platform trend or a collection artifact.

**COIN:** Goes from 27 videos in 2006 to 1,833 by 2017. Almost nothing before 2010, so the cutoff needs to be 2013 or later to leave enough training data.

The main takeaway: a uniform cutoff across all four datasets doesn't work. Each one needs to be set based on its own distribution to avoid badly imbalanced splits.

---

## Background

This started as part of research at Penn State under Dr. Huijuan Xu. Data collection involved scraping YouTube metadata using async functions and BeautifulSoup4 — got processing time down 85% over the naive approach. The experiment design and EDA are done. Actually running the experiments is next once compute is sorted.

If you're working on something related, feel free to open an issue.

---

## Reference

Pegeot et al., *Temporal Dynamics in Visual Data: Analyzing the Impact of Time on Classification Accuracy*, WACV 2025. [[Paper]](https://openaccess.thecvf.com/content/WACV2025/papers/Pegeot_Temporal_Dynamics_in_Visual_Data_Analyzing_the_Impact_of_Time_WACV_2025_paper.pdf)

---

## Stack

Python, PyTorch, OpenCV, NumPy, Pandas, Matplotlib, BeautifulSoup4, yt-dlp

---

**Avanish Grampurohit** — [avanishmg05@gmail.com](mailto:avanishmg05@gmail.com) · [LinkedIn](https://www.linkedin.com/in/avanishmg) · [GitHub](https://github.com/Avanishx05)
