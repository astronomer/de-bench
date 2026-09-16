# Results

One sweep, run in September 2026: every config against every task in the
copperline and upgrade-v2 worlds.

- 139 tasks: 77 copperline, 62 upgrade-v2
- 39 configs across three harnesses (otto, claude-code, codex) and six models
- 17,375 scored trials, three per task and config, five for the four configs marked k=5
- about $15,972 of model spend
- best config: otto-opus-high at 78.8% Pass@1; lowest: claude-code-haiku-low at 19.7%

## What the sweep shows

**The frontier models handle most of this work.** The best configs pass
three in four tasks on the two worlds together, and 78.8% at the top.
Model choice is the largest lever: scores span 20 to 79 points
across the sweep, and the best Haiku config sits 48 points below the best Opus one.
Within Opus 5, across two harnesses and five thinking levels, the spread is 9.5 points.

**The harness sets what a model costs, and part of what it scores.** Holding
model and thinking level fixed and changing only the harness, otto scores
above claude-code on all 9 shared tiers, by 3.9 to 11.0 points of Pass@1.
claude-code costs more per trial on 8 of those 9 tiers, by 11% to
61%; on Sonnet 5 at high thinking it costs 9% less.
Against codex on the GPT models, otto scores above on 6 of 8 tiers, by up to
9.2 points, and loses two Terra tiers by 1.4 points or less. codex is the
cheaper harness there: 14% to 37% less per trial.
On Pass@k, tasks solved at least once, otto leads on all 17 tiers.

**Thinking buys accuracy, and cost rises faster than the score.** Of the
12 harness-and-model pairs run at more than one thinking level, 11 end
higher than they start. The cost of the highest level is two to three times
the lowest on most of them. otto on Opus 5 pays 2.4 times more from minimal
to high thinking for three points of Pass@1.

**On upgrade work the harness gap is widest.** On upgrade-v2, where the task
is knowing what changed between Airflow releases, otto scores above both
other harnesses on all 17 shared tiers, by 3.3 to 20.4 points.
Cheap otto configs beat expensive rivals outright there, not only at matched
price. otto's harness carries Airflow migration knowledge the model was not
trained on, and more reasoning does not substitute for it.

## How to read the numbers

A config is one harness, one model, one thinking level. Each config ran every
task k times. **Pass@1** is the mean fraction of the k trials that passed, and
is the headline. **Pass@k** is the share of tasks solved at least once. **Pass^k**
is the share solved every time, and is the stability floor. Cost is dollars of
model spend per trial at the gateway's rates. Median wall is the median trial
duration in seconds. Pass@1 gaps of about two points are within the movement
a config shows against itself between runs.

A cost marked with `*` has been repriced. The harness priced cache writes at $0
on the gpt-5.6 models, but OpenAI bills those tokens as ordinary input, so the
otto configs on GPT models are corrected by cache-write volume times the input
rate. codex and the Claude models were not affected.

The harness that ran as `otto-dev` in the raw data is the current build of otto,
Astronomer's data-engineering agent, and is labelled `otto` here. Its adapter is
not part of this repo. The results were measured inside Astronomer.

## Cost against accuracy

![Cost per trial against Pass@1](RESULTS.png)

The frontier is the set of configs that nothing else beats on both cost and
pass rate at once. On the two worlds together it holds 13 configs:
8 otto, 5 codex, 0 claude-code.
otto holds every seat on Opus 5 and Sonnet 5. Below about 55% Pass@1 the
frontier is mostly codex on the smaller GPT models, Terra and Luna, which get
there by giving up accuracy.

### Same accuracy, different price

For every claude-code and codex config at 53% Pass@1 or higher, the cheapest
otto config within two points of it.

| rival config               | Pass@1 | $/trial | cheapest otto within 2 points | Pass@1 | $/trial | rival cost / otto cost |
|---|------:|-------:|---|------:|-------:|----:|
| claude-code-opus-xhigh | 75.3% | $2.70 | otto-opus-low | 73.4% | $0.80 | 3.38x |
| claude-code-opus-high | 74.7% | $2.21 | otto-opus-low | 73.4% | $0.80 | 2.76x |
| claude-code-opus | 72.1% | $1.83 | otto-opus-low | 73.4% | $0.80 | 2.29x |
| claude-code-opus-low | 69.3% | $1.07 | otto-opus-low | 73.4% | $0.80 | 1.34x |
| codex-sol-high | 61.1% | $1.20 | otto-sol-low | 63.8% | $0.76\* | 1.58x |
| codex-sol | 60.7% | $0.83 | otto-sol-low | 63.8% | $0.76\* | 1.10x |
| claude-code-sonnet-xhigh | 59.2% | $2.13 | otto-sol-low | 63.8% | $0.76\* | 2.81x |
| claude-code-sonnet-high | 57.1% | $1.65 | otto-sonnet-minimal | 56.4% | $0.65 | 2.54x |
| codex-sol-low | 55.4% | $0.48 | otto-terra-high | 53.5% | $0.52\* | 0.92x |
| claude-code-sonnet | 54.2% | $1.50 | otto-terra-high | 53.5% | $0.52\* | 2.87x |
| codex-terra-high | 53.7% | $0.45 | otto-terra-high | 53.5% | $0.52\* | 0.86x |

## Harness at a fixed model and thinking level

Both worlds together. Same tasks, same trials, same grading; only the harness
changes.

### otto against claude-code

| model | thinking | otto Pass@1 | claude-code Pass@1 | gap | otto Pass^k | claude-code Pass^k | otto Pass@k | claude-code Pass@k | otto $/trial | claude-code $/trial |
|---|---|------:|------:|----:|------:|------:|------:|------:|-------:|-------:|
| claude-opus-5 | low | 73.4% | 69.3% | +4.1 | 57.5% | 55.4% | 86.3% | 79.8% | $0.80 | $1.07 |
| claude-opus-5 | medium | 76.0% | 72.1% | +3.9 | 57.5% | 54.7% | 90.7% | 84.9% | $1.28 | $1.83 |
| claude-opus-5 | high | 78.8% | 74.7% | +4.1 | 60.4% | 55.4% | 90.7% | 89.2% | $1.99 | $2.21 |
| claude-sonnet-5 | low | 54.9% | 45.6% | +9.3 | 38.9% | 31.0% | 69.1% | 59.7% | $0.66 | $0.82 |
| claude-sonnet-5 | medium | 59.0% | 54.2% | +4.8 | 48.2% | 41.7% | 72.6% | 66.9% | $1.03 | $1.50 |
| claude-sonnet-5 | high | 61.4% | 57.1% | +4.3 | 47.5% | 40.3% | 75.5% | 73.4% | $1.82 | $1.65 |
| claude-haiku-4-5 | low | 30.7% | 19.7% | +11.0 | 17.3% | 10.1% | 43.9% | 28.1% | $0.19 | $0.29 |
| claude-haiku-4-5 | medium | 25.9% | 19.9% | +6.0 | 15.1% | 11.5% | 39.6% | 28.8% | $0.18 | $0.29 |
| claude-haiku-4-5 | high | 30.0% | 20.6% | +9.4 | 20.9% | 12.3% | 40.3% | 30.9% | $0.20 | $0.29 |

### otto against codex

| model | thinking | otto Pass@1 | codex Pass@1 | gap | otto Pass^k | codex Pass^k | otto Pass@k | codex Pass@k | otto $/trial | codex $/trial |
|---|---|------:|------:|----:|------:|------:|------:|------:|-------:|-------:|
| gpt-5.6-sol | low | 63.8% | 55.4% | +8.4 | 49.6% | 41.0% | 78.4% | 69.1% | $0.76\* | $0.48 |
| gpt-5.6-sol | medium | 62.3% | 60.7% | +1.6 | 48.2% | 47.5% | 76.3% | 71.9% | $1.11\* | $0.83 |
| gpt-5.6-sol | high | 70.3% | 61.1% | +9.2 | 61.1% | 46.8% | 79.8% | 74.8% | $1.58\* | $1.20 |
| gpt-5.6-terra | low | 47.0% | 48.4% | -1.4 | 31.7% | 36.0% | 62.6% | 60.4% | $0.38\* | $0.30 |
| gpt-5.6-terra | medium | 47.7% | 47.3% | +0.4 | 31.7% | 34.5% | 66.2% | 60.4% | $0.40\* | $0.34 |
| gpt-5.6-terra | high | 53.5% | 53.7% | -0.2 | 36.0% | 42.5% | 70.5% | 66.2% | $0.52\* | $0.45 |
| gpt-5.6-luna | low | 38.1% | 33.1% | +5.0 | 25.9% | 24.5% | 52.5% | 41.0% | $0.10\* | $0.07 |
| gpt-5.6-luna | medium | 43.9% | 42.2% | +1.7 | 30.2% | 31.7% | 58.3% | 53.2% | $0.19\* | $0.13 |

## Thinking level

Each harness-and-model pair from its lowest thinking level to its highest,
both worlds together.

| harness | model | lowest level | highest level | Pass@1 change | cost change |
|---|---|---|---|----:|----:|
| otto | claude-opus-5 | minimal: 75.8% at $0.83 | high: 78.8% at $1.99 | +3.0 | x2.4 |
| otto | claude-sonnet-5 | minimal: 56.4% at $0.65 | high: 61.4% at $1.82 | +5.0 | x2.8 |
| otto | claude-haiku-4-5 | minimal: 30.9% at $0.20 | high: 30.0% at $0.20 | -0.9 | x1.0 |
| otto | gpt-5.6-sol | low: 63.8% at $0.76\* | high: 70.3% at $1.58\* | +6.5 | x2.1 |
| otto | gpt-5.6-terra | low: 47.0% at $0.38\* | high: 53.5% at $0.52\* | +6.5 | x1.4 |
| otto | gpt-5.6-luna | low: 38.1% at $0.10\* | medium: 43.9% at $0.19\* | +5.8 | x1.9 |
| claude-code | claude-opus-5 | low: 69.3% at $1.07 | xhigh: 75.3% at $2.70 | +6.0 | x2.5 |
| claude-code | claude-sonnet-5 | low: 45.6% at $0.82 | xhigh: 59.2% at $2.13 | +13.6 | x2.6 |
| claude-code | claude-haiku-4-5 | low: 19.7% at $0.29 | high: 20.6% at $0.29 | +0.9 | x1.0 |
| codex | gpt-5.6-sol | low: 55.4% at $0.48 | high: 61.1% at $1.20 | +5.7 | x2.5 |
| codex | gpt-5.6-terra | low: 48.4% at $0.30 | high: 53.7% at $0.45 | +5.3 | x1.5 |
| codex | gpt-5.6-luna | low: 33.1% at $0.07 | medium: 42.2% at $0.13 | +9.1 | x1.9 |

## Upgrade-v2 at a fixed model and thinking level

The upgrade world alone. Every shared tier goes to otto.

### otto against claude-code

| model | thinking | otto Pass@1 | claude-code Pass@1 | gap | otto Pass^k | claude-code Pass^k | otto Pass@k | claude-code Pass@k | otto $/trial | claude-code $/trial |
|---|---|------:|------:|----:|------:|------:|------:|------:|-------:|-------:|
| claude-opus-5 | low | 68.3% | 61.3% | +7.0 | 53.2% | 53.2% | 80.6% | 67.7% | $0.34 | $0.49 |
| claude-opus-5 | medium | 68.7% | 62.3% | +6.4 | 51.6% | 50.0% | 83.9% | 72.6% | $0.62 | $0.82 |
| claude-opus-5 | high | 73.5% | 66.5% | +7.0 | 58.1% | 53.2% | 85.5% | 79.0% | $1.13 | $1.24 |
| claude-sonnet-5 | low | 54.8% | 45.2% | +9.6 | 40.3% | 35.5% | 69.4% | 54.8% | $0.30 | $0.40 |
| claude-sonnet-5 | medium | 62.9% | 47.3% | +15.6 | 51.6% | 30.6% | 77.4% | 64.5% | $0.50 | $0.72 |
| claude-sonnet-5 | high | 62.4% | 59.1% | +3.3 | 46.8% | 43.5% | 77.4% | 72.6% | $0.82 | $1.08 |
| claude-haiku-4-5 | low | 48.4% | 28.0% | +20.4 | 29.0% | 14.5% | 64.5% | 41.9% | $0.11 | $0.22 |
| claude-haiku-4-5 | medium | 38.7% | 26.3% | +12.4 | 21.0% | 14.5% | 59.7% | 40.3% | $0.08 | $0.21 |
| claude-haiku-4-5 | high | 47.3% | 28.5% | +18.8 | 35.5% | 19.4% | 59.7% | 41.9% | $0.12 | $0.22 |

### otto against codex

| model | thinking | otto Pass@1 | codex Pass@1 | gap | otto Pass^k | codex Pass^k | otto Pass@k | codex Pass@k | otto $/trial | codex $/trial |
|---|---|------:|------:|----:|------:|------:|------:|------:|-------:|-------:|
| gpt-5.6-sol | low | 68.3% | 48.9% | +19.4 | 54.8% | 38.7% | 79.0% | 59.7% | $0.58\* | $0.25 |
| gpt-5.6-sol | medium | 57.5% | 47.3% | +10.2 | 41.9% | 38.7% | 71.0% | 53.2% | $0.73\* | $0.48 |
| gpt-5.6-sol | high | 74.2% | 54.8% | +19.4 | 67.7% | 46.8% | 80.6% | 61.3% | $1.21\* | $0.72 |
| gpt-5.6-terra | low | 46.8% | 42.5% | +4.3 | 29.0% | 32.3% | 66.1% | 53.2% | $0.28\* | $0.17 |
| gpt-5.6-terra | medium | 44.6% | 40.9% | +3.7 | 27.4% | 30.6% | 64.5% | 53.2% | $0.29\* | $0.21 |
| gpt-5.6-terra | high | 54.8% | 47.3% | +7.5 | 35.5% | 38.7% | 72.6% | 58.1% | $0.38\* | $0.25 |
| gpt-5.6-luna | low | 51.6% | 36.6% | +15.0 | 37.1% | 32.3% | 67.7% | 40.3% | $0.07\* | $0.04 |
| gpt-5.6-luna | medium | 46.2% | 40.3% | +5.9 | 33.9% | 33.9% | 61.3% | 48.4% | $0.11\* | $0.07 |

## Standings

### Copperline and upgrade-v2 together

| # | config | model | thinking | k | Pass@1 | Pass@k | Pass^k | $/trial | median wall | frontier |
|--:|---|---|---|--:|------:|------:|------:|-------:|-----------:|:--:|
| 1 | otto-opus-high | claude-opus-5 | high | 5 | 78.8% | 90.7% | 60.4% | $1.99 | 354s | yes |
| 2 | otto-opus | claude-opus-5 | medium | 5 | 76.0% | 90.7% | 57.5% | $1.28 | 248s | yes |
| 3 | otto-opus-minimal | claude-opus-5 | minimal | 3 | 75.8% | 87.0% | 59.7% | $0.83 | 154s | yes |
| 4 | claude-code-opus-xhigh | claude-opus-5 | xhigh | 3 | 75.3% | 83.5% | 64.7% | $2.70 | 421s |  |
| 5 | claude-code-opus-high | claude-opus-5 | high | 5 | 74.7% | 89.2% | 55.4% | $2.21 | 376s |  |
| 6 | otto-opus-low | claude-opus-5 | low | 3 | 73.4% | 86.3% | 57.5% | $0.80 | 163s | yes |
| 7 | claude-code-opus | claude-opus-5 | medium | 5 | 72.1% | 84.9% | 54.7% | $1.83 | 259s |  |
| 8 | otto-sol-high | gpt-5.6-sol | high | 3 | 70.3% | 79.8% | 61.1% | $1.58\* | 317s |  |
| 9 | claude-code-opus-low | claude-opus-5 | low | 3 | 69.3% | 79.8% | 55.4% | $1.07 | 283s |  |
| 10 | otto-sol-low | gpt-5.6-sol | low | 3 | 63.8% | 78.4% | 49.6% | $0.76\* | 141s | yes |
| 11 | otto-sol | gpt-5.6-sol | medium | 3 | 62.3% | 76.3% | 48.2% | $1.11\* | 256s |  |
| 12 | otto-sonnet-high | claude-sonnet-5 | high | 3 | 61.4% | 75.5% | 47.5% | $1.82 | 414s |  |
| 13 | codex-sol-high | gpt-5.6-sol | high | 3 | 61.1% | 74.8% | 46.8% | $1.20 | 211s |  |
| 14 | codex-sol | gpt-5.6-sol | medium | 3 | 60.7% | 71.9% | 47.5% | $0.83 | 160s |  |
| 15 | claude-code-sonnet-xhigh | claude-sonnet-5 | xhigh | 3 | 59.2% | 72.7% | 44.6% | $2.13 | 442s |  |
| 16 | otto-sonnet | claude-sonnet-5 | medium | 3 | 59.0% | 72.6% | 48.2% | $1.03 | 257s |  |
| 17 | claude-code-sonnet-high | claude-sonnet-5 | high | 3 | 57.1% | 73.4% | 40.3% | $1.65 | 391s |  |
| 18 | otto-sonnet-minimal | claude-sonnet-5 | minimal | 3 | 56.4% | 71.2% | 38.9% | $0.65 | 158s | yes |
| 19 | codex-sol-low | gpt-5.6-sol | low | 3 | 55.4% | 69.1% | 41.0% | $0.48 | 87s | yes |
| 20 | otto-sonnet-low | claude-sonnet-5 | low | 3 | 54.9% | 69.1% | 38.9% | $0.66 | 193s |  |
| 21 | claude-code-sonnet | claude-sonnet-5 | medium | 3 | 54.2% | 66.9% | 41.7% | $1.50 | 273s |  |
| 22 | codex-terra-high | gpt-5.6-terra | high | 3 | 53.7% | 66.2% | 42.5% | $0.45 | 143s | yes |
| 23 | otto-terra-high | gpt-5.6-terra | high | 3 | 53.5% | 70.5% | 36.0% | $0.52\* | 191s |  |
| 24 | codex-terra-low | gpt-5.6-terra | low | 3 | 48.4% | 60.4% | 36.0% | $0.30 | 96s | yes |
| 25 | otto-terra | gpt-5.6-terra | medium | 3 | 47.7% | 66.2% | 31.7% | $0.40\* | 164s |  |
| 26 | codex-terra | gpt-5.6-terra | medium | 3 | 47.3% | 60.4% | 34.5% | $0.34 | 118s |  |
| 27 | otto-terra-low | gpt-5.6-terra | low | 3 | 47.0% | 62.6% | 31.7% | $0.38\* | 118s |  |
| 28 | claude-code-sonnet-low | claude-sonnet-5 | low | 3 | 45.6% | 59.7% | 31.0% | $0.82 | 287s |  |
| 29 | otto-luna | gpt-5.6-luna | medium | 3 | 43.9% | 58.3% | 30.2% | $0.19\* | 178s | yes |
| 30 | codex-luna | gpt-5.6-luna | medium | 3 | 42.2% | 53.2% | 31.7% | $0.13 | 121s | yes |
| 31 | otto-luna-low | gpt-5.6-luna | low | 3 | 38.1% | 52.5% | 25.9% | $0.10\* | 84s | yes |
| 32 | codex-luna-low | gpt-5.6-luna | low | 3 | 33.1% | 41.0% | 24.5% | $0.07 | 52s | yes |
| 33 | otto-haiku-minimal | claude-haiku-4-5 | minimal | 3 | 30.9% | 43.2% | 20.2% | $0.20 | 127s |  |
| 34 | otto-haiku-low | claude-haiku-4-5 | low | 3 | 30.7% | 43.9% | 17.3% | $0.19 | 125s |  |
| 35 | otto-haiku-high | claude-haiku-4-5 | high | 3 | 30.0% | 40.3% | 20.9% | $0.20 | 128s |  |
| 36 | otto-haiku | claude-haiku-4-5 | medium | 3 | 25.9% | 39.6% | 15.1% | $0.18 | 146s |  |
| 37 | claude-code-haiku-high | claude-haiku-4-5 | high | 3 | 20.6% | 30.9% | 12.3% | $0.29 | 110s |  |
| 38 | claude-code-haiku | claude-haiku-4-5 | medium | 3 | 19.9% | 28.8% | 11.5% | $0.29 | 108s |  |
| 39 | claude-code-haiku-low | claude-haiku-4-5 | low | 3 | 19.7% | 28.1% | 10.1% | $0.29 | 108s |  |

### Copperline

| # | config | model | thinking | k | Pass@1 | Pass@k | Pass^k | $/trial | median wall | frontier |
|--:|---|---|---|--:|------:|------:|------:|-------:|-----------:|:--:|
| 1 | otto-opus-high | claude-opus-5 | high | 5 | 83.1% | 94.8% | 62.3% | $2.69 | 501s | yes |
| 2 | claude-code-opus-xhigh | claude-opus-5 | xhigh | 3 | 83.1% | 92.2% | 70.1% | $3.61 | 566s |  |
| 3 | otto-opus | claude-opus-5 | medium | 5 | 81.8% | 96.1% | 62.3% | $1.81 | 352s | yes |
| 4 | claude-code-opus-high | claude-opus-5 | high | 5 | 81.3% | 97.4% | 57.1% | $2.99 | 539s |  |
| 5 | claude-code-opus | claude-opus-5 | medium | 5 | 80.0% | 94.8% | 58.4% | $2.64 | 396s |  |
| 6 | otto-opus-minimal | claude-opus-5 | minimal | 3 | 78.8% | 92.2% | 61.0% | $1.13 | 227s | yes |
| 7 | otto-opus-low | claude-opus-5 | low | 3 | 77.5% | 90.9% | 61.0% | $1.17 | 227s |  |
| 8 | claude-code-opus-low | claude-opus-5 | low | 3 | 75.8% | 89.6% | 57.1% | $1.53 | 473s |  |
| 9 | codex-sol | gpt-5.6-sol | medium | 3 | 71.4% | 87.0% | 54.5% | $1.11 | 189s | yes |
| 10 | otto-sol-high | gpt-5.6-sol | high | 3 | 67.1% | 79.2% | 55.8% | $1.88\* | 414s |  |
| 11 | otto-sol | gpt-5.6-sol | medium | 3 | 66.2% | 80.5% | 53.2% | $1.40\* | 306s |  |
| 12 | codex-sol-high | gpt-5.6-sol | high | 3 | 66.2% | 85.7% | 46.8% | $1.59 | 270s |  |
| 13 | codex-sol-low | gpt-5.6-sol | low | 3 | 60.6% | 76.6% | 42.9% | $0.67 | 111s | yes |
| 14 | otto-sonnet-high | claude-sonnet-5 | high | 3 | 60.6% | 74.0% | 48.1% | $2.62 | 560s |  |
| 15 | otto-sol-low | gpt-5.6-sol | low | 3 | 60.2% | 77.9% | 45.5% | $0.90\* | 170s |  |
| 16 | claude-code-sonnet | claude-sonnet-5 | medium | 3 | 59.7% | 68.8% | 50.6% | $2.13 | 428s |  |
| 17 | codex-terra-high | gpt-5.6-terra | high | 3 | 58.9% | 72.7% | 45.5% | $0.61 | 186s | yes |
| 18 | claude-code-sonnet-xhigh | claude-sonnet-5 | xhigh | 3 | 58.0% | 72.7% | 42.9% | $2.65 | 607s |  |
| 19 | otto-sonnet | claude-sonnet-5 | medium | 3 | 55.8% | 68.8% | 45.5% | $1.45 | 344s |  |
| 20 | claude-code-sonnet-high | claude-sonnet-5 | high | 3 | 55.4% | 74.0% | 37.7% | $2.10 | 582s |  |
| 21 | otto-sonnet-low | claude-sonnet-5 | low | 3 | 55.0% | 68.8% | 37.7% | $0.95 | 254s |  |
| 22 | codex-terra-low | gpt-5.6-terra | low | 3 | 53.2% | 66.2% | 39.0% | $0.40 | 120s | yes |
| 23 | codex-terra | gpt-5.6-terra | medium | 3 | 52.4% | 66.2% | 37.7% | $0.44 | 134s |  |
| 24 | otto-terra-high | gpt-5.6-terra | high | 3 | 52.4% | 68.8% | 36.4% | $0.62\* | 258s |  |
| 25 | otto-sonnet-minimal | claude-sonnet-5 | minimal | 3 | 51.5% | 64.9% | 37.7% | $0.91 | 239s |  |
| 26 | otto-terra | gpt-5.6-terra | medium | 3 | 50.2% | 67.5% | 35.1% | $0.49\* | 185s |  |
| 27 | otto-terra-low | gpt-5.6-terra | low | 3 | 47.2% | 59.7% | 33.8% | $0.46\* | 155s |  |
| 28 | claude-code-sonnet-low | claude-sonnet-5 | low | 3 | 45.9% | 63.6% | 27.3% | $1.15 | 485s |  |
| 29 | codex-luna | gpt-5.6-luna | medium | 3 | 43.7% | 57.1% | 29.9% | $0.18 | 141s | yes |
| 30 | otto-luna | gpt-5.6-luna | medium | 3 | 42.0% | 55.8% | 27.3% | $0.25\* | 207s |  |
| 31 | codex-luna-low | gpt-5.6-luna | low | 3 | 30.3% | 41.6% | 18.2% | $0.10 | 60s | yes |
| 32 | otto-luna-low | gpt-5.6-luna | low | 3 | 27.3% | 40.3% | 16.9% | $0.13\* | 101s |  |
| 33 | otto-haiku-low | claude-haiku-4-5 | low | 3 | 16.5% | 27.3% | 7.8% | $0.25 | 174s |  |
| 34 | otto-haiku-high | claude-haiku-4-5 | high | 3 | 16.0% | 24.7% | 9.1% | $0.26 | 175s |  |
| 35 | otto-haiku | claude-haiku-4-5 | medium | 3 | 15.6% | 23.4% | 10.4% | $0.26 | 169s |  |
| 36 | otto-haiku-minimal | claude-haiku-4-5 | minimal | 3 | 14.7% | 24.7% | 7.8% | $0.26 | 175s |  |
| 37 | claude-code-haiku | claude-haiku-4-5 | medium | 3 | 14.7% | 19.5% | 9.1% | $0.36 | 161s |  |
| 38 | claude-code-haiku-high | claude-haiku-4-5 | high | 3 | 14.3% | 22.1% | 6.5% | $0.35 | 156s |  |
| 39 | claude-code-haiku-low | claude-haiku-4-5 | low | 3 | 13.0% | 16.9% | 6.5% | $0.35 | 154s |  |

### Upgrade-v2

| # | config | model | thinking | k | Pass@1 | Pass@k | Pass^k | $/trial | median wall | frontier |
|--:|---|---|---|--:|------:|------:|------:|-------:|-----------:|:--:|
| 1 | otto-sol-high | gpt-5.6-sol | high | 3 | 74.2% | 80.6% | 67.7% | $1.21\* | 197s | yes |
| 2 | otto-opus-high | claude-opus-5 | high | 5 | 73.5% | 85.5% | 58.1% | $1.13 | 171s | yes |
| 3 | otto-opus-minimal | claude-opus-5 | minimal | 3 | 72.0% | 80.6% | 58.1% | $0.45 | 63s | yes |
| 4 | otto-opus | claude-opus-5 | medium | 5 | 68.7% | 83.9% | 51.6% | $0.62 | 118s |  |
| 5 | otto-opus-low | claude-opus-5 | low | 3 | 68.3% | 80.6% | 53.2% | $0.34 | 84s | yes |
| 6 | otto-sol-low | gpt-5.6-sol | low | 3 | 68.3% | 79.0% | 54.8% | $0.58\* | 104s |  |
| 7 | claude-code-opus-high | claude-opus-5 | high | 5 | 66.5% | 79.0% | 53.2% | $1.24 | 173s |  |
| 8 | claude-code-opus-xhigh | claude-opus-5 | xhigh | 3 | 65.6% | 72.6% | 58.1% | $1.58 | 242s |  |
| 9 | otto-sonnet | claude-sonnet-5 | medium | 3 | 62.9% | 77.4% | 51.6% | $0.50 | 150s |  |
| 10 | otto-sonnet-minimal | claude-sonnet-5 | minimal | 3 | 62.4% | 79.0% | 40.3% | $0.33 | 57s | yes |
| 11 | otto-sonnet-high | claude-sonnet-5 | high | 3 | 62.4% | 77.4% | 46.8% | $0.82 | 232s |  |
| 12 | claude-code-opus | claude-opus-5 | medium | 5 | 62.3% | 72.6% | 50.0% | $0.82 | 89s |  |
| 13 | claude-code-opus-low | claude-opus-5 | low | 3 | 61.3% | 67.7% | 53.2% | $0.49 | 47s |  |
| 14 | claude-code-sonnet-xhigh | claude-sonnet-5 | xhigh | 3 | 60.8% | 72.6% | 46.8% | $1.48 | 236s |  |
| 15 | claude-code-sonnet-high | claude-sonnet-5 | high | 3 | 59.1% | 72.6% | 43.5% | $1.08 | 153s |  |
| 16 | otto-sol | gpt-5.6-sol | medium | 3 | 57.5% | 71.0% | 41.9% | $0.73\* | 195s |  |
| 17 | otto-sonnet-low | claude-sonnet-5 | low | 3 | 54.8% | 69.4% | 40.3% | $0.30 | 117s | yes |
| 18 | otto-terra-high | gpt-5.6-terra | high | 3 | 54.8% | 72.6% | 35.5% | $0.38\* | 107s |  |
| 19 | codex-sol-high | gpt-5.6-sol | high | 3 | 54.8% | 61.3% | 46.8% | $0.72 | 137s |  |
| 20 | otto-luna-low | gpt-5.6-luna | low | 3 | 51.6% | 67.7% | 37.1% | $0.07\* | 63s | yes |
| 21 | otto-haiku-minimal | claude-haiku-4-5 | minimal | 3 | 51.1% | 66.1% | 35.5% | $0.12 | 68s |  |
| 22 | codex-sol-low | gpt-5.6-sol | low | 3 | 48.9% | 59.7% | 38.7% | $0.25 | 57s |  |
| 23 | otto-haiku-low | claude-haiku-4-5 | low | 3 | 48.4% | 64.5% | 29.0% | $0.11 | 64s |  |
| 24 | otto-haiku-high | claude-haiku-4-5 | high | 3 | 47.3% | 59.7% | 35.5% | $0.12 | 70s |  |
| 25 | codex-terra-high | gpt-5.6-terra | high | 3 | 47.3% | 58.1% | 38.7% | $0.25 | 89s |  |
| 26 | codex-sol | gpt-5.6-sol | medium | 3 | 47.3% | 53.2% | 38.7% | $0.48 | 124s |  |
| 27 | claude-code-sonnet | claude-sonnet-5 | medium | 3 | 47.3% | 64.5% | 30.6% | $0.72 | 81s |  |
| 28 | otto-terra-low | gpt-5.6-terra | low | 3 | 46.8% | 66.1% | 29.0% | $0.28\* | 71s |  |
| 29 | otto-luna | gpt-5.6-luna | medium | 3 | 46.2% | 61.3% | 33.9% | $0.11\* | 143s |  |
| 30 | claude-code-sonnet-low | claude-sonnet-5 | low | 3 | 45.2% | 54.8% | 35.5% | $0.40 | 40s |  |
| 31 | otto-terra | gpt-5.6-terra | medium | 3 | 44.6% | 64.5% | 27.4% | $0.29\* | 138s |  |
| 32 | codex-terra-low | gpt-5.6-terra | low | 3 | 42.5% | 53.2% | 32.3% | $0.17 | 66s |  |
| 33 | codex-terra | gpt-5.6-terra | medium | 3 | 40.9% | 53.2% | 30.6% | $0.21 | 99s |  |
| 34 | codex-luna | gpt-5.6-luna | medium | 3 | 40.3% | 48.4% | 33.9% | $0.07 | 97s | yes |
| 35 | otto-haiku | claude-haiku-4-5 | medium | 3 | 38.7% | 59.7% | 21.0% | $0.08 | 118s |  |
| 36 | codex-luna-low | gpt-5.6-luna | low | 3 | 36.6% | 40.3% | 32.3% | $0.04 | 42s | yes |
| 37 | claude-code-haiku-high | claude-haiku-4-5 | high | 3 | 28.5% | 41.9% | 19.4% | $0.22 | 53s |  |
| 38 | claude-code-haiku-low | claude-haiku-4-5 | low | 3 | 28.0% | 41.9% | 14.5% | $0.22 | 50s |  |
| 39 | claude-code-haiku | claude-haiku-4-5 | medium | 3 | 26.3% | 40.3% | 14.5% | $0.21 | 43s |  |

Regenerate this page with `python tools/render_results.py`; the data is
`results/sweep-2026-09.json`.
