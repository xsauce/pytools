# -*- coding: utf-8 -*-
import io
import zipfile

from util.tool_func import md5


class InMemoryZip(object):
    def __init__(self, allow_zip64=False):
        self.allow_zip64 = allow_zip64
        self.in_memory_zip = io.BytesIO()

    def append(self, filename_in_zip, file_contents):
        zf = zipfile.ZipFile(self.in_memory_zip, 'a', zipfile.ZIP_DEFLATED, allowZip64=self.allow_zip64)
        zf.writestr(filename_in_zip, file_contents)
        for zfile in zf.filelist:
            zfile.create_system = 0

    def read_all(self):
        self.in_memory_zip.seek(0)
        return self.in_memory_zip.read()

    def md5(self):
        return md5(self.in_memory_zip.getvalue())

    def stream(self, buffer_bytes_size):
        self.in_memory_zip.seek(0)
        while 1:
            block = self.in_memory_zip.read(buffer_bytes_size)
            if block == '':
                break
            yield block
            # self.in_memory_zip.seek(buffer_bytes_size, 1)

    def length(self):
        # self.in_memory_zip.seek(0)
        return len(self.in_memory_zip.getvalue())

    def save_to_file(self, file_name):
        with open(file_name, 'wb') as f:
            for b in self.stream(1024):
                f.write(b)


if __name__ == '__main__':
    s = '/Users/sam/Desktop/A20161101104423647.zip'
    print zipfile.ZipFile(s, 'r').getinfo('busi.xml')