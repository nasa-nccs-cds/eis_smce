import intake
from eis_smce.data.intake.catalog import cm
# intake.output_notebook()

bucketname = 'eis-dh-fire'
cat = cm().cat(bucketname)
print( f" cat = {list(cat)}" )