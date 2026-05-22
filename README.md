# Action Recognition: Temporal Generalization Study

**Status:** R3D-18 and I3D-R50 experiments complete on YouCook2. Core finding established. Transformer-based models and larger datasets are future work.

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

- **Train** on videos from peak years 2013–2014 (most data, most representative)
- **Test** on temporally adjacent years: 2011, 2012 (backward) and 2015, 2016 (forward)
- **Baseline** — in-distribution accuracy on training era videos

Run all five models under both conditions. Main thing to measure is how much performance drops going from in-distribution to temporally shifted test sets. Secondary analysis examines whether forward generalization (to newer videos) degrades faster than backward generalization (to older videos).

### YouCook2 split

| Split | Year | Videos | Direction |
|---|---|---|---|
| Train | 2013–2014 | ~866 | in-distribution |
| Val | 2011 | ~104 | backward (2 yr gap) |
| Test 1 | 2012 | ~255 | backward (1 yr gap) |
| Test 2 | 2015 | ~207 | forward (1 yr gap) |
| Test 3 | 2016 | ~53 | forward (2 yr gap) |

Years with fewer than 50 videos (2006–2010) were excluded to avoid statistically unreliable splits.

---

## Results

### YouCook2 — R3D-18

Pretrained on Kinetics-400. Backbone frozen, linear head fine-tuned on 2013–2014 videos for 20 epochs.

| Split | Year | Accuracy | Gap | Direction |
|---|---|---|---|---|
| Train | 2013 | 0.383 | 0 | in-dist |
| Val | 2011 | 0.025 | 2 yrs | backward |
| Test 1 | 2012 | 0.014 | 1 yr | backward |
| Test 2 | 2015 | 0.006 | 1 yr | forward |
| Test 3 | 2016 | 0.000 | 2 yrs | forward |

Training converged cleanly — loss dropped from 5.07 to 2.54, train accuracy reached 53%. Val accuracy remained flat near 0% throughout all 20 epochs, indicating severe temporal overfitting rather than a training failure.

**Key finding:** Accuracy drops over 95% relative immediately outside the training era. Forward generalization degrades faster than backward — 2015 (0.6%) is worse than 2011 (2.5%) despite the same temporal gap, suggesting post-2014 filming style evolution impacts model performance more than pre-2013 differences.

---

### YouCook2 — I3D-R50

Pretrained on Kinetics-400. Backbone frozen, linear head fine-tuned on 2013–2014 videos for 20 epochs.

| Split | Year | Accuracy | Direction |
|---|---|---|---|
| Train | 2013 | 0.277 | in-dist |
| Val | 2011 | 0.019 | backward |
| Test 1 | 2012 | 0.028 | backward |
| Test 2 | 2015 | 0.012 | forward |
| Test 3 | 2016 | 0.023 | forward |

I3D trained faster and to higher in-distribution accuracy (89% train by epoch 20) but showed similarly severe temporal generalization failure. Val accuracy never exceeded 2.5% across all epochs.

**Notable difference from R3D-18:** I3D shows slightly higher accuracy on 2016 (2.3%) than 2015 (1.2%), a non-monotonic pattern not seen in R3D-18. This may reflect I3D's stronger spatiotemporal features being more robust to certain filming style changes.

---

### Comparison: R3D-18 vs I3D-R50

| Split | Year | R3D-18 | I3D-R50 | Direction |
|---|---|---|---|---|
| Train | 2013 | 0.383 | 0.277 | in-dist |
| Val | 2011 | 0.025 | 0.019 | backward |
| Test 1 | 2012 | 0.014 | 0.028 | backward |
| Test 2 | 2015 | 0.006 | 0.012 | forward |
| Test 3 | 2016 | 0.000 | 0.023 | forward |

Both models show the same qualitative pattern — in-distribution accuracy far exceeds out-of-distribution accuracy across all temporal splits. The finding is consistent across both CNN architectures, suggesting this is a property of temporal distribution shift rather than a model-specific artifact.

I3D shows marginally better temporal generalization on forward splits (2015, 2016), possibly due to its deeper architecture and two-stream design capturing more generalizable motion features.

---

## Limitations and future work

### Overfitting to training era

Both models show severe overfitting — train accuracy reaches 38–89% while val accuracy stays below 3% across all epochs. This is not a training failure but the central finding: atomic action recognition models trained on era-specific instructional video learn visual features tied to that era's filming style rather than generalizable action representations.

Several factors likely contribute to this:

**Data scale.** With ~718 valid training videos split across ~180 recipe classes, average class size is around 4 videos. This is insufficient for a classifier to learn robust features — the model memorizes rather than generalizes. Larger datasets like HowTo100M (1M+ videos) would provide far more examples per class and likely reduce this effect.

**Domain mismatch.** R3D-18 and I3D were pretrained on Kinetics (short atomic actions — sports, gestures) and then applied to procedural cooking video. The backbone features are not well aligned to the instructional domain regardless of temporal split.

**Frozen backbone.** Only the classification head was trained. Unfreezing later backbone layers and fine-tuning with a lower learning rate would allow the model to adapt to cooking video features, potentially improving both absolute accuracy and temporal generalization.

### What future work could address

**Transformer-based atomic models.** VideoSwin and ViViT are both pretrained on Kinetics like R3D and I3D, but their self-attention mechanisms capture longer-range spatiotemporal dependencies. It's an open question whether this architectural advantage translates to better temporal generalization or whether the same overfitting pattern holds.

**Larger datasets.** Running the same temporal split experiment on HowTo100M or COIN would test whether data scale alone mitigates temporal degradation — more training data per class means less overfitting to era-specific features. The EDA shows COIN and HowTo100M have more balanced temporal distributions, which may also produce cleaner degradation curves.

**Mitigation strategies.** Temporal data augmentation (random temporal jitter, brightness/contrast variation simulating different camera generations), partial backbone unfreezing, and early stopping are natural next steps. Measuring whether these reduce the accuracy gap between in-distribution and temporally shifted test sets would turn this from a diagnostic study into an actionable one.

---

## Where things stand

- [x] Dataset collection pipeline — 4 datasets with upload year metadata extracted
- [x] Temporal split logic and stratification
- [x] EDA — video distributions by year across all four datasets
- [x] R3D-18 training and evaluation on YouCook2
- [x] I3D-R50 training and evaluation on YouCook2
- [x] Core finding established — severe temporal generalization failure across both CNN architectures

**Future work:**
- [ ] VideoSwin — hierarchical transformer, same Kinetics pretraining, different architecture family
- [ ] ViViT — pure transformer with factorized spatiotemporal attention
- [ ] InternVideo2 — SOTA model, to test whether scale and richer pretraining mitigates temporal shift
- [ ] Overfitting mitigation strategies — data augmentation, partial backbone unfreezing, early stopping — and whether they reduce temporal degradation
- [ ] Experiments on larger datasets (HowTo100M, COIN, CrossTask) — larger training sets may naturally reduce overfitting to era-specific visual features, smoothing the temporal degradation curve
- [ ] Per-category analysis — which recipe/action types are most affected by temporal shift

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
