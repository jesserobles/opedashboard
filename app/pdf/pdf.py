import os
import re

from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfpage import PDFTextExtractionNotAllowed
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfdevice import PDFDevice
from pdfminer.layout import LAParams
from pdfminer.converter import PDFPageAggregator
import pdfminer



class PDF(object):
    """A wrapper for pdfminer.six.
    
    Args:
        filepath (str): The path to the pdf file.
    
    """
    
    def __init__(self, filepath):
        self._filepath = filepath
        self._pages = []
        self._page_boxes = []

        # Pdfminer boilerplate
        self.file = open(self._filepath, 'rb')
        self.parser = PDFParser(self.file)
        self.document = PDFDocument(self.parser)
        if not self.document.is_extractable:
            raise PDFTextExtractionNotAllowed
        self.rsrcmgr = PDFResourceManager()
        
        # BEGIN LAYOUT ANALYSIS
        # Set parameters for analysis.
        self.laparams = LAParams()
        self.device = PDFPageAggregator(self.rsrcmgr, laparams=self.laparams)
        self.interpreter = PDFPageInterpreter(self.rsrcmgr, self.device)
        
        # Loop through pages and get page dimensions and text lines with coordinates
        for page in PDFPage.create_pages(self.document):
            self._page_boxes.append(page.mediabox)
            self.interpreter.process_page(page)
            layout = self.device.get_result()
            lines = [[obj.get_text(), (obj.bbox)] for obj in layout._objs 
                      if isinstance(obj, pdfminer.layout.LTTextBoxHorizontal)]
            self._pages.append(lines)
        self._page_dimensions = [(page[2] - page[0], page[3] - page[0]) 
                                for page in self._page_boxes]
        self._text = self._get_text()
        self._page_texts = self._get_text(all_=False)
        self._page_count = len(self._page_texts)
        self._cleanup()
    
    
    def _get_text(self, all_=True):
        if all_:
            text = ' '.join([line[0] for page in self._pages for line in page])
            # Clean up multiple spaces, leaving newlines intact
            text = re.sub(' {2,}', ' ', text)
        else:
            text = [re.sub(' {2,}', ' ', ' '.join([line[0] for line in page]))
                    for page in self._pages]
        return text
    
    
    def text_coords(self, index):
        """Returns a list of lists for page at index.
           Eacn inner list contains the text string, followed by a tuple
           with the coordinates of the text bounding box.
        """
        return self._pages[index]
    
    
    @property
    def text(self):
        """Returns the full text of the PDF."""
        return self._text
    
    
    @property
    def pages(self):
        """Returns a list of page texts."""
        return list(self._page_texts)
    
    
    @property
    def page_count(self):
        """Returns the pdf page count."""
        return self._page_count
    
    
    @property
    def page_dimensions(self):
        """Returns a list of tuples, each one a pages dimensions."""
        return self._page_dimensions
    
    
    @property
    def page_boxes(self):
        """Returns a list of tuples, each one a pages dimensions."""
        return self._page_boxes
    

    @property
    def filepath(self):
        """Returns the filepath of the original pdf."""
        return self._filepath

    
    def _cleanup(self):
        """Closes pdf file and deletes unneccessary attributes."""
        self.file.close()
        del self.file
        del self.parser
        del self.document
        del self.rsrcmgr
        del self.laparams
        del self.device
        del self.interpreter
    
    
    
    def __getitem__(self, index):
        return self._page_texts[index]
    
    
    def __repr__(self):
        return f"<PDF '{os.path.splitext(os.path.basename(self._filepath))[0]}'>"
