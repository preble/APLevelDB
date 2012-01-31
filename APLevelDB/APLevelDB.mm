//
//  APLevelDB.m
//
//  Created by Adam Preble on 1/23/12.
//  Copyright (c) 2012 Adam Preble. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in
//	all copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//	THE SOFTWARE.

//
//  Portions of APLevelDB are based on LevelDB-ObjC:
//	https://github.com/hoisie/LevelDB-ObjC
//  Specifically the SliceFromString/StringFromSlice macros, and the structure of
//  the enumeration methods.  License for those potions follows:
//
//	Copyright (c) 2011 Pave Labs
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in
//	all copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//	THE SOFTWARE.
//

#import "APLevelDB.h"

#import <leveldb/db.h>
#import <leveldb/options.h>

NSString * const APLevelDBErrorDomain = @"APLevelDBErrorDomain";

#define SliceFromString(_string_) (leveldb::Slice((char *)[_string_ UTF8String], [_string_ lengthOfBytesUsingEncoding:NSUTF8StringEncoding]))
#define StringFromSlice(_slice_) ([[NSString alloc] initWithBytes:_slice_.data() length:_slice_.size() encoding:NSUTF8StringEncoding])

#pragma mark - APLevelDB

@interface APLevelDB () {
	leveldb::DB *mDB;
	leveldb::ReadOptions mReadOptions;
	leveldb::WriteOptions mWriteOptions;
}
- (id)initWithPath:(NSString *)path error:(NSError **)errorOut;
+ (leveldb::Options)defaultCreateOptions;
@property (nonatomic, readonly) leveldb::DB *db;
@end


@implementation APLevelDB

@synthesize path = mPath;
@synthesize db=mDB;

+ (APLevelDB *)levelDBWithPath:(NSString *)path error:(NSError **)errorOut
{
	return [[APLevelDB alloc] initWithPath:path error:errorOut];
}

- (id)initWithPath:(NSString *)path error:(NSError **)errorOut
{
	if ((self = [super init]))
	{
		mPath = path;
		
		leveldb::Options options = [[self class] defaultCreateOptions];
		
		leveldb::Status status = leveldb::DB::Open(options, [mPath UTF8String], &mDB);

		if (!status.ok())
		{
			if (errorOut)
			{
				NSString *statusString = [[NSString alloc] initWithCString:status.ToString().c_str() encoding:NSUTF8StringEncoding];
				*errorOut = [NSError errorWithDomain:APLevelDBErrorDomain
												code:0
											userInfo:[NSDictionary dictionaryWithObjectsAndKeys:statusString, NSLocalizedDescriptionKey, nil]];
			}
			return nil;
		}
		
		mWriteOptions.sync = false;
	}
	return self;
}

- (void)dealloc
{
	delete mDB;
	mDB = NULL;
}

+ (leveldb::Options)defaultCreateOptions
{
	leveldb::Options options;
	options.create_if_missing = true;
	return options;
}

- (BOOL)setData:(NSData *)data forKey:(NSString *)key
{
	leveldb::Slice keySlice = SliceFromString(key);
	leveldb::Slice valueSlice = leveldb::Slice((const char *)[data bytes], (size_t)[data length]);
	leveldb::Status status = mDB->Put(mWriteOptions, keySlice, valueSlice);
	return (status.ok() == true);
}

- (BOOL)setString:(NSString *)str forKey:(NSString *)key
{
	// This could have been based on 
	leveldb::Slice keySlice = SliceFromString(key);
	leveldb::Slice valueSlice = SliceFromString(str);
	leveldb::Status status = mDB->Put(mWriteOptions, keySlice, valueSlice);
	return (status.ok() == true);
}

- (NSData *)dataForKey:(NSString *)key
{
	leveldb::Slice keySlice = SliceFromString(key);
	std::string valueCPPString;
	leveldb::Status status = mDB->Get(mReadOptions, keySlice, &valueCPPString);

	if (!status.ok())
		return nil;
	else
		return [NSData dataWithBytes:valueCPPString.data() length:valueCPPString.size()];
}

- (NSString *)stringForKey:(NSString *)key
{
	leveldb::Slice keySlice = SliceFromString(key);
	std::string valueCPPString;
	leveldb::Status status = mDB->Get(mReadOptions, keySlice, &valueCPPString);
	
	// We assume (dangerously?) UTF-8 string encoding:
	if (!status.ok())
		return nil;
	else
		return [[NSString alloc] initWithBytes:valueCPPString.data() length:valueCPPString.size() encoding:NSUTF8StringEncoding];
}

- (BOOL)removeKey:(NSString *)key
{
	leveldb::Slice keySlice = SliceFromString(key);
	leveldb::Status status = mDB->Delete(mWriteOptions, keySlice);
	return (status.ok() == true);
}

- (NSArray *)allKeys
{
	NSMutableArray *keys = [NSMutableArray array];
	[self enumerateKeys:^(NSString *key, BOOL *stop) {
		[keys addObject:key];
	}];
	return keys;
}

- (void)enumerateKeysAndValuesAsStrings:(void (^)(NSString *key, NSString *value, BOOL *stop))block
{
	BOOL stop = NO;
	leveldb::Iterator* iter = mDB->NewIterator(leveldb::ReadOptions());
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		leveldb::Slice key = iter->key(), value = iter->value();
		NSString *k = StringFromSlice(key);
		NSString *v = [[NSString alloc] initWithBytes:value.data() length:value.size() encoding:NSUTF8StringEncoding];
		block(k, v, &stop);
		if (stop)
			break;
	}
	
	delete iter;
}

- (void)enumerateKeys:(void (^)(NSString *key, BOOL *stop))block
{
	BOOL stop = NO;
	leveldb::Iterator* iter = mDB->NewIterator(leveldb::ReadOptions());
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		leveldb::Slice key = iter->key();
		NSString *k = StringFromSlice(key);
		block(k, &stop);
		if (stop)
			break;
	}
	
	delete iter;
}


@end


#pragma mark - APLevelDBIterator

@interface APLevelDBIterator () {
	leveldb::Iterator *mIter;
}
@end



@implementation APLevelDBIterator

+ (id)iteratorWithLevelDB:(APLevelDB *)db
{
	APLevelDBIterator *iter = [[[self class] alloc] initWithLevelDB:db];
	return iter;
}

- (id)initWithLevelDB:(APLevelDB *)db
{
	if ((self = [super init]))
	{
		mIter = db.db->NewIterator(leveldb::ReadOptions());
		mIter->SeekToFirst();
	}
	return self;
}

- (id)init
{
	[NSException raise:@"BadInitializer" format:@"Use the designated initializer, -initWithLevelDB:, instead."];
	return nil;
}

- (void)dealloc
{
	delete mIter;
	mIter = NULL;
}

- (BOOL)seekToKey:(NSString *)key
{
	leveldb::Slice target = SliceFromString(key);
	mIter->Seek(target);
	return mIter->Valid() == true;
}

- (void)seekToFirst
{
	mIter->SeekToFirst();
}

- (void)seekToLast
{
	mIter->SeekToLast();
}

- (NSString *)nextKey
{
	mIter->Next();
	return [self key];
}

- (NSString *)key
{
	if (mIter->Valid() == false)
		return nil;
	leveldb::Slice value = mIter->key();
	return StringFromSlice(value);
}

- (NSString *)valueAsString
{
	if (mIter->Valid() == false)
		return nil;
	leveldb::Slice value = mIter->value();
	return StringFromSlice(value);
}

- (NSData *)valueAsData
{
	if (mIter->Valid() == false)
		return nil;
	leveldb::Slice value = mIter->value();
	return [NSData dataWithBytes:value.data() length:value.size()];
}

@end
