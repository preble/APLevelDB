# APLevelDB

A stylish Objective-C wrapper for [LevelDB][].  What's LevelDB?  It describes itself as: "a fast key-value storage library written at Google that provides an ordered mapping from string keys to string values."  LevelDB is made available under the [New BSD License][].

APLevelDB is comprised of two classes, `APLevelDB` and `APLevelDBIterator`.  It is written to be compiled under ARC.  APLevelDB was written by [Adam Preble][].

## Usage

Fetch the LevelDB submodule:

	git submodule init
	git submodule update

Compile LevelDB for Mac:

	cd leveldb
	CXXFLAGS=-stdlib=libc++ make

Or for iOS:

	cd leveldb
	CXXFLAGS=-stdlib=libc++ make PLATFORM=IOS

Open `APLevelDB.xcodeproj` and run the unit tests (Product, Test) or compile the APLevelDB framework and add it to your project.

Note about the `CXXFLAGS` variable: By default leveldb appears to build with libstdc++, where Xcode projects build with libc++. In order to avoid setting the C++ Standard Library build setting, I build leveldb this way.

### Creating An Instance

	NSString *path = ...;
	NSError *err;
	APLevelDB *db = [APLevelDB levelDBWithPath:path error:&err];

### Getting and Setting Values

	[db setString:@"the value" forKey:@"key"];
	NSString *value = [db stringForKey:@"key"];

### Iterating

You can iterate over the entire database:

	[db enumerateKeysAndValuesAsStrings:^(NSString *key, NSString *value, BOOL *stop) {
		NSLog(@"key=%@ value=%@", key, value);
	}];	

Or just over its keys:

	[db enumerateKeys:^(NSString *key, BOOL *stop) {
		NSLog(@"key=%@", key);
	}];	

One of LevelDB's most useful features is that its keys are ordered. Using `APLevelDBIterator` you can iterate over the keys in order, and even start at a certain key using `-seekToKey:`:

	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:db];
	[iter seekToKey:@"seek-key"]; // optional
	NSString *key;
	while ((key = [iter nextKey]))
	{
		NSString *value = [iter valueAsString];
		...
	}

### Storing Objects

APLevelDB uses strings for keys, but its values can be interpreted as UTF-8 strings, or as raw data:

	NSString *value = [db stringForKey:@"key"];
	NSData *valueData = [db dataForKey:@"key"];

You can use this to archive objects into and out of APLevelDB:

	id obj = ...;
	NSData *data = [NSKeyedArchiver archivedDataWithRootObject:obj];
	[db setData:data forKey:@"obj"];
	
	obj = [NSKeyedUnarchiver unarchiveObjectWithData:[db dataForKey:@"obj"]];


## License

Copyright (c) 2012 Adam Preble. All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

Portions of APLevelDB are based on [LevelDB-ObjC][].  Its license can be found in `APLevelDB.mm`.

[LevelDB]: http://code.google.com/p/leveldb/
[Adam Preble]: http://adampreble.net/
[LevelDB-ObjC]: https://github.com/hoisie/LevelDB-ObjC
[New BSD License]: http://www.opensource.org/licenses/bsd-license.php
