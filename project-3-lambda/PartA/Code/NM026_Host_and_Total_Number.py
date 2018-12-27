## Count the name tag in a file, after that output a file with that count

filename = input()
dic = {}
with open(filename) as fp:
    for line in fp:
        word = line.split()[0]
        dic[word] = dic.get(word, 0)
        dic[word] += 1

with open('records_{}'.format(filename),'w') as new_file:
    for word in sorted(num_dic):
        num = dic[word]
        new_file.write('{} {}\n'.format(word, num))
        