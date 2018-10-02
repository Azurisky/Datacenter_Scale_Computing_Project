f = open("stop_words_list_new.txt", "r")
f2 = open("stop_words_list_array.txt", "w")
ans = "["
for x in f:
  	ans += "{},".format(x)
  	# print(x)
ans += "]"
f2.write(ans)