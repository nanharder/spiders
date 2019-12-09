import jieba
import codecs
import numpy
from collections import Counter

from PIL import Image
import matplotlib.pyplot as plt
from wordcloud import WordCloud, ImageColorGenerator
import configs


def get_words(txt):
    seg_list = jieba.cut(txt, cut_all=False)
    stop = [line.strip() for line in open(r'resources/stop_word.txt').readlines()]

    c = Counter()
    word_list = []
    for word in seg_list:
        if len(word) > 1 and word not in stop:
            word_list.append(word)
            c[word] += 1

    for (k, v) in c.most_common(100):
        with open(config['statfile_path'], 'a+', encoding='utf-8') as f:
            f.write(k + ' ' + str(v))
            f.write('\n')
    return ' '.join(word_list)


def generate_wordcloud(wordlist):
    print('正在生成词云')

    bg = numpy.array(Image.open(config['source_img']))
    wc = WordCloud(background_color='white', margin=2, mask=bg,
                   max_words=200, max_font_size=50, font_path=r'resources/fzlantingbold.ttf',
                   min_font_size=8, mode='RGBA', colormap='pink')
    wc.generate(wordlist)

    imagecolors = ImageColorGenerator(bg)
    plt.imshow(wc.recolor(color_func=imagecolors))
    plt.axis('off')
    plt.figure()
    plt.axis('off')
    plt.imshow(bg, cmap=plt.cm.gray)

    wc.to_file(config['generate_img'])
    print('生成完毕')


if __name__ == '__main__':
    config = configs.word_stat_config
    with codecs.open(config['word_file_path'], 'r', 'utf8') as f:
        txt = f.read()
    wordlist = get_words(txt)
    generate_wordcloud(wordlist)
