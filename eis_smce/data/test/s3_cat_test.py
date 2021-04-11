import intake
from eis_smce.data.intake.catalog import cm
# intake.output_notebook()

bucketname = 'eis-dh-fire'

cm = cm( bucket = bucketname )
cat = cm.cat()
print( f" cat = {list(cat)}" )