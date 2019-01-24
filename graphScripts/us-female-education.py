import matplotlib.pyplot as plt
import numpy as np
import sys

if len(sys.argv) != 2:
    print('usage: us-female-education.py outputPath')
else:
    outputPath = sys.argv[1]
    bachelorsYears = []
    bachelorsPercentages = []
    mastersYears = []
    mastersPercentages = []
    doctoralYears = []
    doctoralPercentages = []

    with open(file=outputPath) as outputFile:
        data = outputFile.readlines()
        for line in data:
            splitLine = line.split('\t')
            educationLevel = splitLine[0]
            percentageDifference = float(splitLine[1])
            yearOfData = int(splitLine[2])

            if educationLevel == "Bachelor's":
                for b in range(len(bachelorsYears) + 1):
                    if b == len(bachelorsYears) or yearOfData < bachelorsYears[b]:
                        bachelorsPercentages.insert(b, percentageDifference)
                        bachelorsYears.insert(b, yearOfData)
            elif educationLevel == "Master's":
                for m in range(len(mastersYears) + 1):
                    if m == len(mastersYears) or yearOfData < mastersYears[m]:
                        mastersPercentages.insert(m, percentageDifference)
                        mastersYears.insert(m, yearOfData)
            elif educationLevel == 'Doctoral':
                for d in range(len(doctoralYears) + 1):
                    if d == len(doctoralYears) or yearOfData < doctoralYears[d]:
                        doctoralPercentages.insert(d, percentageDifference)
                        doctoralYears.insert(d, yearOfData)
    
    plt.xlabel('Year')
    plt.ylabel('Change in Education Attainment (p.p.)')
    plt.suptitle('Change in United States Female Education since 2000')
    plt.plot(bachelorsYears, bachelorsPercentages, 'b.-', label="Bachelor's")
    plt.plot(mastersYears, mastersPercentages, 'g.-', label="Master's")
    plt.plot(doctoralYears, doctoralPercentages, 'r.-', label='Doctoral')
    plt.legend()
    plt.show()