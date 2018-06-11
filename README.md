# JumbleWords
The jumble puzzle is a common newspaper puzzle, it contains a series of anagrams that must be solved (see https://en.wikipedia.org/wiki/Jumble). To solve, one must solve each of the individual jumbles. The circled letters are then used to create an additional anagram to be solved. In especially difficult versions, some of the anagrams in the first set can possess multiple solutions. To get the final answer, it is important to know all possible anagrams of a given series of letters.

This program has been written using spark + scala.

Words dictionary has been declared as broadcast variable to be availble to all nodes without being computed at each node.

Output of the program is:
```json
puzzle id: 1
words: gland, major, becalm, lawyer
final words: and, been, well
puzzle id: 2
words: blend, avoid, sychee, camera
final words: are, year, day
puzzle id: 3
words: stash, rodeo, indict, italic
final words: each, decision
puzzle id: 4
words: dinky, galei, encore, devout
final words: toddling
puzzle id: 5
words: trying, divert, seaman, deceit, shadow, heckle
final words: events, interest
```
