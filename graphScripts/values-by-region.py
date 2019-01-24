import matplotlib.pyplot as plt
import numpy as np
import sys

if len(sys.argv) != 6:
    print('usage: percentage-by-region.py outputPath countryDataPath nameOfDataOutput unit logFlag')
else:
    outputPath = sys.argv[1] # path to the MapReduce output
    countryDataPath = sys.argv[2] # path to the Country Data
    countryMap = {} # key = country name, value = region country is a part of
    countryRegions = [] # country region list for the scatter plot
    countryPercentages = [] # country percentages

    with open(file=countryDataPath) as countryFile:
        data = countryFile.readlines()
        for line in data:
            splitLine = line.split('","')
            countryName = splitLine[2] # country name is in the third field
            regionName = splitLine[7] # region name is in the eighth field

            if len(regionName) > 0:            
                countryMap[countryName] = regionName # assign the region name to the country
    
    with open(file=outputPath) as outputFile:
        data = outputFile.readlines()
        for line in data:
            splitLine = line.split('\t')
            countryName = splitLine[0] # country name is in the first field
            percentage = float(splitLine[1]) # percentage is in the second field

            if countryName in countryMap:
                countryRegions.append(countryMap[countryName])
                countryPercentages.append(percentage)

    plt.xlabel('Region')
    plt.ylabel(sys.argv[3] + ' ' + sys.argv[4])
    plt.title(sys.argv[3] + ' by Region')
    plt.scatter(countryRegions, countryPercentages)
    if(int(sys.argv[5]) > 0):
        plt.yscale('log')
    plt.show()