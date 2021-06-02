from pathlib import Path

# Returns the path of the directory, where your script file is placed
mypath = Path().absolute()
print(f'Absolute path: {mypath}')

mypath1 = Path().absolute().parent
print(f'Absolute path parent: {mypath1}')

print( f'Source path: {mypath1}/source' )