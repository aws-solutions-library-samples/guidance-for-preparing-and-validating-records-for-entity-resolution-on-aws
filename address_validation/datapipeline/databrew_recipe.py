import json
import boto3
import os

def get_recipe_contents(file_path: str):
    with open(file_path,"r") as fp:
        return json.load(fp)

def add_update_recipe(recipe_name: str, file_path: str, cli_profile: str, force_publish: bool = False):
    recipe_contents=get_recipe_contents(file_path)

    try:
        # client creation
        session = boto3.Session(profile_name=cli_profile)
        databrew_client = session.client('databrew')
        # getting file name and contents
        recipe_lists = databrew_client.list_recipes(MaxResults=99)
        if recipe_name not in (x['Name'] for x in recipe_lists['Recipes']):
            print("recipe doesn't exist, Creating and publishing")
            # creating recipe and publishing it
            databrew_client.create_recipe(Name=recipe_name, Steps=recipe_contents)
            databrew_client.publish_recipe(Description='publishing recipe', Name=recipe_name)
        elif recipe_name in (x['Name'] for x in recipe_lists['Recipes']) and force_publish:
            print("recipe already exists, Updating and publishing")
            # updating recipe
            databrew_client.update_recipe(Description='updating recipe',Name=recipe_name,Steps= recipe_contents)
            # publishing a recipe
            databrew_client.publish_recipe(Description='publishing recipe', Name=recipe_name)
        else:
            print("recipe already exists, not updating or publishing")
    except Exception as e:
        raise e

def publish_recipe(recipe_name: str, cli_profile: str):
    try:
        # client creation
        session = boto3.Session(profile_name=cli_profile)
        databrew_client = session.client('databrew')
        recipe_lists = databrew_client.list_recipes(MaxResults=99)
        if recipe_name in (x['Name'] for x in recipe_lists['Recipes']):
            databrew_client.publish_recipe(Description='publishing recipe', Name=recipe_name)
    except Exception as e:
        raise e

if __name__ == "__main__":
    add_update_recipe(recipe_name='idres-normalize-data',file_path='runtime/databrew/normalize-data-recipe.json',cli_profile='cleanroom1', force_publish=True)
