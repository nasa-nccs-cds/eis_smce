import intake
from eis_smce.data.storage.s3 import s3m
from data.intake.catalog import cm
# intake.output_notebook()

bucketname = 'eis-dh-fire'

cm = cm( bucket = bucketname )
cat = s3m().cat()
print( f" cat = {list(cat)}" )