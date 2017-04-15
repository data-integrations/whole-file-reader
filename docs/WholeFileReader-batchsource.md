# Whole File Source

Whole file source reads the entire file. It's highly recommended not
to use this plugin when the files are very large and splittable.

## Usage Notes

If a path specified is a directory, then one file is read at a time
and passed along, but if the path specifies a file, then that exact
file is read and passed along.
