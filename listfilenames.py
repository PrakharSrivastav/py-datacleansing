import glob

# the absolute path where all the files are location
abs_path = "/home/prakhar/Projects/order_947089/*/*.csv"

# list to hold the names of all the files
file_names = []

# for all the files in the absolute path
for filename in glob.iglob(abs_path):
    # only if the list does not contain the latest file name
    if filename[55:] not in file_names:
        # add the file name to the list
        file_names.append(filename[55:])

# open a file
f = open("tablenames.txt","w",512,"utf-8",None,'\n')
# print out the name of the files
for names in file_names:
    # print(names)
    # print(names[:-4])
    newName = names[6:-4]
    f.write(newName.upper() + '_full\n')

f.close()