#-*- coding:utf-8 -*-
#Author:李明洲
#@Time:2020/3/28 10:24

class TrieNode:
    def __init__(self):
        self.nodes = dict()  # 构建字典
        self.is_leaf = False
    def insert(self, word: str):
        curr = self
        for char in word:
            if char not in curr.nodes:
                curr.nodes[char] = TrieNode()
            curr = curr.nodes[char]
        curr.is_leaf = True
    def insert_many(self, words: [str]):
        for word in words:
            self.insert(word)
    def search(self, word: str):
        curr = self
        for char in word:
            if char not in curr.nodes:
                return False
            curr = curr.nodes[char]
        return curr.is_leaf

trie=TrieNode()
trie.insert("hello")
trie.insert("word")
print(trie.search("llo"))