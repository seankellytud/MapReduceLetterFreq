import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import csv



########################## USING PANDAS ################################
#convert txt file to csv
with open('AverageOccurrences.txt') as infile, open('AverageOccurrences.csv', 'w') as outfile:
    for line in infile:
        outfile.write(" ".join(line.split()).replace(' ', ','))
        outfile.write(",") #seems to add an extra empty column, but can be ignored
        outfile.write("\n")

df = pd.read_csv("AverageOccurrences.csv", header =None) #read the text file which has no header so first line is not read as column name

df.columns =['Language', 'Letter', 'Frequency', ' '] #set column names


#split into separate csv files according to language column values
csv_english = df[df['Language'] == 'English']
csv_french = df[df['Language'] == 'French']
csv_polish = df[df['Language'] == 'Polish']

eng= csv_english.to_csv('english.csv', index=False, header= None)
fr=csv_french.to_csv('french.csv', index=False, header= None)
pl=csv_polish.to_csv('polish.csv', index=False, header= None) # split into separate csvs


eng = pd.read_csv("english.csv", header =None)
fr = pd.read_csv("french.csv", header =None)
pl = pd.read_csv("polish.csv", header =None)

eng.columns =['Language', 'Letter', 'Frequency', ' '] #set column names for the new csv files
fr.columns =['Language', 'Letter', 'Frequency', ' '] 
pl.columns =['Language', 'Letter', 'Frequency', ' '] 



################################## MATPLOTLIB PART ##################################

engx = eng['Letter'].tolist()
engy = eng['Frequency'].tolist()
plt.bar(engx,engy)
plt.show()

frx = fr['Letter'].tolist()
fry = fr['Frequency'].tolist()
plt.bar(frx,fry)
plt.show()

plx = pl['Letter'].tolist()
ply = pl['Frequency'].tolist()
plt.bar(plx,ply)
plt.show()



    

