l = ['my cat\n', 'your dog\n']

for x in l:
    print x.find("\n")

new_list = [x for x in l if x.find("\n") > -1]

print new_list