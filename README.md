# Classification-of-semantic-relations
implementation of a research paper using map-reduce pattern, machine learning tools and experiments on a large-scale input

## introduction
implementation of the paper of Dmitry Davidov and Ari Rappoport [Classification of Semantic Relationships between Nominals Using Pattern Clusters](http://www.cs.huji.ac.il/~arir/nominals.pdf).

## Implementation notes
For classification, we used WEKA software.

## Input
*We extracted the patterns from Google's English 5Gram data set
*The training and the testing of the classifier based on the [BLESS dataset](https://www.cs.bgu.ac.il/~dsp181/wiki.files/dataset.txt)

## Experiment
The evaluation in the paper is task 4 of SemEval-07 - identification of 6 semantic relation between nouns (Cause-Effect, Instrument-Agency, Product-Producer, Origin-Entity, Theme-Tool, Part-Whole, Content-Container), as described in Sections 2.5 and 5.1.

 instead, we evaluated the system on a different task - classification of the following semantic relations:
 * COORD: the first noun is a co-hyponym (coordinate) of the second noun, i.e., they belong to the same semantic class: alligator-coord-lizard.
 *HYPER: the first noun is a hypernym of the second noun: alligator-hyper-animal.
 *MERO: the first noun refers to a part/component/organ/member of the second noun, or something that the second noun contains or is made of: alligator-mero-mouth.
 *RANDOM: the two nouns are not semantically related.

## Reports
The reports can be found on https://github.com/eladshamailov/Classification-of-semantic-relations/blob/master/Reports.pdf
