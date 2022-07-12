import random


def get_fields():
    with open("data/field.txt", encoding='utf-8') as f:
        content = f.readlines()
    i = random.randint(0, len(content) - 1)
    content = [x.strip() for x in content]

    return content

#


def get_types():
    with open("data/type.txt") as f:
        content = f.readlines()
    i = random.randint(0, len(content) - 1)
    content = [x.strip() for x in content]
    type = content[i]
    return type
