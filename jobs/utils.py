from .logger import Logger
import json
import os
import argparse


class Utility:
    """Project Utility class"""
    def __init__(self):
        self.config = None
        
    def getConfig(self):
        return self.config
    
    def loadConfig(self,path:str):
        with open(path, "r") as config_file:
            self.config = json.load(config_file)
            
    def _parseArguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--config", required=True, type=str, default="../config/config.json")
        args = parser.parse_args()
        self.loadConfig(args.config)
    