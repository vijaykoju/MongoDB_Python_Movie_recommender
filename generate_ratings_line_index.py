import csv

fname = 'ratings.csv'

# read rating data of movies by all users and print the starting and
# ending line number of each user
with open(fname, 'r') as f:
    reader = csv.reader(f, delimiter=',')
    next(reader)  # skip the heading
    ln = 1
    item = next(reader)
    key = item[0]
    ln += 1
    print(ln)
    for item in reader:
        ln += 1
        key1 = item[0]
        if key != key1:  # check if the user changes
            print(ln)
        key = key1
