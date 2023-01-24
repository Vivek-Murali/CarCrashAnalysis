import json
import os
from typing import String, List, Dict, Any, Boolean

class Loader:
    """
    Loader class to load data from CSV files.
    """
    def __init__(self):
        self.version = None # version
        
    def __str__(self):
        return ""
    
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        pass
    
    def __repr__(self):
        pass
    
    def readCsvFile(self, path:String)->Boolean:
        pass
    
    def writeCsvFile(self, path:String)->Boolean:
        """_summary_

        Args:
            path (String): _description_

        Returns:
            Boolean: _description_
        """
        pass