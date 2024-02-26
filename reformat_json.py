import json
import re
import os
def acceptinputs():
    inpath = input('Input json Directory \n')
    outpath = input('Specify output directory for jsonlines \n')
    return([inpath,outpath])


def main():
    inputs = acceptinputs()
    alljson = [f for f in os.listdir(inputs[0]) if f.endswith(".json")]
    for i in alljson:
        content = open(os.path.join(inputs[0],i), "r").read() 
        j_content = re.sub('\n *',"",content)
        j_content = j_content.replace("}{","}\n{").replace(" : ",":")
        j_content = j_content+"\n"
        with open(os.path.join(inputs[1],i+"l"), "w") as outfile:
            outfile.write(j_content) 

main()