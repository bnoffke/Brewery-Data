import pandas as pd
import random
from datetime import datetime

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

globalBeverages = [] #Need to make this a stored list of beverages from a pickle or something to keep as our bev operational database

def beverageGen():
    beverage_type = random.choice(beverageMetaData['type'])
    firstWord = random.choice(beverageMetaData['firstWord'])
    secondWord = random.choice(beverageMetaData['secondWord'])
    name = ' '.join([firstWord,secondWord,beverage_type])
    return(beverage_type, name)

class Beverage():
    def __init__(self,brewery_id) -> None:
        self.brewery_id = brewery_id
        self.beverage_type, self.name = beverageGen()
        self.id = hash(f'{self.brewery_id}|{self.name}')
        self.is_active = True
        self.created_at = datetime.today()
        self.updated_at = datetime.today()

def pullBreweries(client,table_id):
    breweries = client.query(f'select id,brewery_type from {table_id}').to_dataframe()

def assignBeverages(breweries):

    for _,row in breweries.iterrows():
        capMin = beverageMetaData['breweryCapacities'][row.brewery_type][0]
        capMax = beverageMetaData['breweryCapacities'][row.brewery_type][1]
        for i in range(random.randrange(capMin,capMax)):
            curBeverage = Beverage(row.id)
            globalBeverages.append(curBeverage)
    return(globalBeverages)
    #Eventually get this to retire beers when the max cap is exceeded for existing entries

def makeBeveragesDataFrame():
    cols = ['id','name','beverage_type','brewery_id','is_active','created_at','updated_at']
    
    
    bevsList = []
    for bev in globalBeverages:
        bevList = []
        for col in cols:
            bevList.append(getattr(bev,col))
        bevsList.append(bevList)
    dfBeverages = pd.DataFrame(bevsList,columns = cols)
    return(dfBeverages)


def main():
    pass

if __name__ == '__main__':
    main()