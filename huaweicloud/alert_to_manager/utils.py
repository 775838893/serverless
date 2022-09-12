# -*- coding:utf-8 -*-

import os
from pypinyin import lazy_pinyin

os.environ['PYPINYIN_NO_PHRASES'] = 'true'  # 禁用内置的词组拼音库，减少内存开销
os.environ['PYPINYIN_NO_DICT_COPY'] = 'true'  # 禁用默认的“拼音库”copy 操作，减少内存开销


def hanzi2pinyin(keyword):
    """
    中文转拼音
    @param keyword: 中文
    @return:
    """
    result = "".join(lazy_pinyin(keyword)).split()[0]
    return result


if __name__ == '__main__':
    print(hanzi2pinyin("华为云"))
