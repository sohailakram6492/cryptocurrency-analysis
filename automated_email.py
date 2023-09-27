import pandas as pd
cards = str(['Jack', 8, 2, 6, 'King', 5, 3, 'Queen'])
def insertion_sort(cards):
    length = len(cards)
    for i in range(1, length):
        indexi = cards[i]
        indexj = i - 1
        print(indexi, "  ", indexj)
        while indexj >= 0 and indexi < cards[indexj]:
            cards[indexj+1] = cards[indexj]
            indexj = indexj - 1
        cards[indexj+1] = indexi

#driver or execution code
insertion_sort(cards)
print(cards)
