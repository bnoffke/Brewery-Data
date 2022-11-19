import pandas as pd
import random
from datetime import datetime

#This dictionary defines the possible words that can be randomly chosen for generating beverages
beverageMetaData = {
    'firstWord' : ['Super',
                    'Mega',
                    'Dry',
                    'Double',
                    'Whiskey',
                    'Sour',
                    'Smokey',
                    'Hazy'
                    ],
    'secondWord' : ['Fruited',
                    'Hopped',
                    'Juicy',
                    'Barrel',
                    'Lazy',
                    'Squishy',
                    'Red',
                    'Pale',
                    'Spicy',
                    'Vanilla',
                    'Chocolate',
                    'Scotch',
                    'Dry',
                    'Citrus',
                    'Smoothie',
                    'Smoked'
                    ],
    'type' : ['IPA',
                'Stout',
                'Sour',
                'Saison',
                'Ale',
                'Lager',
                'Pilsner',
                'Selzter',
                'Hefeweizen',
                'Tripel',
                'Dubbel',
                'Quad'
                ],
    #Each tuple defines the min/max beverage capacities per brewery type
    'breweryCapacities': {
                            'micro' : (5,10),
                            'brewpub' : (10,15),
                            'contract' : (15,20),
                            'planning' : (0,5),
                            'regional' : (15,20),
                            'large' : (20,30),
                            'proprietor' : (15,20),
                            'nano' : (2,6)

                        }
    }

#This is a placeholder for what will eventually be a persistent store of generate beverages, with procedures to retire beverages
#For now, all beverages are generated fresh with each run
globalBeverages = []

def beverageGen():
    #Pick a random set of words from our pre-defined beverage metadata
    beverage_type = random.choice(beverageMetaData['type'])
    firstWord = random.choice(beverageMetaData['firstWord'])
    secondWord = random.choice(beverageMetaData['secondWord'])
    name = ' '.join([firstWord,secondWord,beverage_type])
    return(beverage_type, name)

class Beverage():
    #A class to define a beverage and its attributes
    #Eventually there will be methods to handle retiring a beverage from a breweries inventory
    def __init__(self,brewery_id) -> None:
        self.brewery_id = brewery_id
        self.beverage_type, self.name = beverageGen()
        self.id = hash(f'{self.brewery_id}|{self.name}')
        self.is_active = True
        self.created_at = datetime.today()
        self.updated_at = datetime.today()

def assignBeverages(breweries):
    #Given a dataframe of breweries, assign random beverages based on the capacity of the brewery
    #Eventually get this to retire beers when the max cap is exceeded for existing entries
    for _,row in breweries.iterrows():
        #For the current brewery, find the minimum and maximum beverage capacities based on the brewery's type
        capMin = beverageMetaData['breweryCapacities'][row.brewery_type][0]
        capMax = beverageMetaData['breweryCapacities'][row.brewery_type][1]

        #Gather set of beverage IDs to ensure we don't create duplicates
        #This will be an empty set until a persistent beverage system is implemented
        bevIdSet = set([ bev.id for bev in globalBeverages])

        #Pick a random number in the possible range for beverage capacity
        for i in range(random.randrange(capMin,capMax)):
            #Generate a random beverage
            curBeverage = Beverage(row.id)
            
            #Only accept the beverage if it is unique to the brewery
            if curBeverage.id not in bevIdSet:
                globalBeverages.append(curBeverage)
                bevIdSet.add(curBeverage.id)
    return(globalBeverages)
    

def makeBeveragesDataFrame():
    #Take the list of beverage objects and create a dataframe to prepare for loading into a BigQuery table

    #Update this list with beverage attributes that should be in the dataframe
    cols = ['id','name','beverage_type','brewery_id','is_active','created_at','updated_at']

    #Initialize a list to be a list of lists that will facilitate conversion to a dataframe    
    bevsList = []

    #Loop through the generated beverages
    for bev in globalBeverages:
        #Organize beverage attributes into a list
        bevList = [getattr(bev,col) for col in cols]
        bevsList.append(bevList)

    #Convert list of lists into a dataframe
    dfBeverages = pd.DataFrame(bevsList,columns = cols)
    return(dfBeverages)

def main():
    pass

if __name__ == '__main__':
    main()