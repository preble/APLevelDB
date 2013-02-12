//
//  APLevelDBTests.m
//  APLevelDBTests
//
//  Created by Adam Preble on 8/14/12.
//  Copyright (c) 2012 Adam Preble. All rights reserved.
//

#import <SenTestingKit/SenTestingKit.h>
#import "APLevelDB.h"

@interface APLevelDBTests : SenTestCase {
	APLevelDB *_db;
	NSData *_largeData;
}

@end

@implementation APLevelDBTests

- (void)setUp
{
    [super setUp];
    
    // Set-up code here.
	
	NSString *path = [NSTemporaryDirectory() stringByAppendingPathComponent:@"test.leveldb"];
	
	if ([[NSFileManager defaultManager] fileExistsAtPath:path])
		[[NSFileManager defaultManager] removeItemAtPath:path error:nil];
	
	_db = [APLevelDB levelDBWithPath:path error:nil];
}

- (void)tearDown
{
    // Tear-down code here.
	_db = nil;
    
    [super tearDown];
}

#pragma mark - Tests

- (void)testSetStringForKey
{
	NSString *text = @"Hello";
	NSString *key = @"key";
	[_db setString:text forKey:key];
	
	STAssertEqualObjects(text, [_db stringForKey:key], @"Error retrieving string for key.");
}

- (void)testSetDataForKey
{
	// Create some test data using NSKeyedArchiver:
	NSData *data = [NSKeyedArchiver archivedDataWithRootObject:[NSDate date]];
	NSString *key = @"key";
	[_db setData:data forKey:key];
	
	NSData *fetched = [_db dataForKey:key];
	STAssertNotNil(fetched, @"Key for data not found.");
	STAssertEqualObjects(data, fetched, @"Fetched data doesn't match original data.");
}

- (void)testNilForUnknownKey
{
	STAssertNil([_db stringForKey:@"made up key"], @"stringForKey: should return nil if a key doesn't exist");
	STAssertNil([_db dataForKey:@"another made up key"], @"dataForKey: should return nil if a key doesn't exist");
}

- (void)testRemoveKey
{
	NSString *text = @"Hello";
	NSString *key = @"key";
	[_db setString:text forKey:key];
	
	STAssertEqualObjects(text, [_db stringForKey:key], @"stringForKey should have returned the original text");
	
	[_db removeKey:key];
	
	STAssertNil([_db stringForKey:key], @"stringForKey should return nil after removal of key");
	STAssertNil([_db dataForKey:key], @"dataForKey should return nil after removal of key");
}

- (void)testAllKeys
{
	NSDictionary *keysAndValues = [self populateWithUUIDsAndReturnDictionary];

	NSArray *sortedOriginalKeys = [keysAndValues.allKeys sortedArrayUsingSelector:@selector(compare:)];
	STAssertEqualObjects(sortedOriginalKeys, [_db allKeys], @"");
}

- (void)testEnumeration
{
	NSDictionary *keysAndValues = [self populateWithUUIDsAndReturnDictionary];
	NSArray *sortedOriginalKeys = [keysAndValues.allKeys sortedArrayUsingSelector:@selector(compare:)];
	
	__block NSUInteger keyIndex = 0;
	[_db enumerateKeys:^(NSString *key, BOOL *stop) {
		STAssertEqualObjects(key, sortedOriginalKeys[keyIndex], @"enumerated key does not match");
		keyIndex++;
	}];
}

- (void)testEnumerationUsingStrings
{
	NSDictionary *keysAndValues = [self populateWithUUIDsAndReturnDictionary];
	NSArray *sortedOriginalKeys = [keysAndValues.allKeys sortedArrayUsingSelector:@selector(compare:)];
	
	__block NSUInteger keyIndex = 0;
	[_db enumerateKeysAndValuesAsStrings:^(NSString *key, NSString *value, BOOL *stop) {
		
		NSString *originalKey = sortedOriginalKeys[keyIndex];
		STAssertEqualObjects(key, originalKey, @"enumerated key does not match");
		STAssertEqualObjects(value, keysAndValues[originalKey], @"enumerated value does not match");
		
		keyIndex++;
	}];
}

- (void)testSubscripting
{
	NSString *text = @"Hello";
	NSString *key = @"key";
	_db[key] = text;
	
	STAssertEqualObjects(text, _db[key], @"Error retrieving string for key.");
}

- (void)testSubscriptingNilForUnknownKey
{
	STAssertNil(_db[@"no such key as this key"], @"Subscripting access should return nil for an unknown key.");
}

- (void)testSubscriptingAccessException
{
	id output;
	STAssertThrowsSpecificNamed(output = _db[ [NSDate date] ], NSException, NSInvalidArgumentException, @"Subscripting with non-NSString type should raise an NSInvalidArgumentException.");
}
- (void)testSubscriptingAssignmentException
{
	NSData *someData = [NSKeyedArchiver archivedDataWithRootObject:[NSDate date]];
	STAssertThrowsSpecificNamed(_db[ [NSDate date] ] = @"hello", NSException, NSInvalidArgumentException, @"Subscripting with non-NSString type should raise an NSInvalidArgumentException.");
	STAssertThrowsSpecificNamed(_db[ @"valid key" ] = [NSDate date], NSException, NSInvalidArgumentException, @"Subscripting with non-NSString type should raise an NSInvalidArgumentException.");
	STAssertNoThrow(_db[ @"valid key" ] = @"hello", @"Subscripting with non-NSString type should raise an NSInvalidArgumentException.");
	STAssertNoThrow(_db[ @"valid key" ] = someData, @"Subscripting with non-NSString type should raise an NSInvalidArgumentException.");
}

- (void)testLargeValue
{
	NSString *key = @"key";
	NSData *data = [self largeData];
	
	[_db setData:data forKey:key];
	STAssertEqualObjects(data, [_db dataForKey:key], @"Data read from database does not match original.");
}

#pragma mark - Tests - Iterators

- (void)testIteratorNilOnEmptyDatabase
{
	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:_db];
	STAssertNil(iter, @"Iterator should be nil for an empty database.");
}

- (void)testIteratorNotNilOnPopulatedDatabase
{
	_db[@"a"] = @"1";
	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:_db];
	STAssertNotNil(iter, @"Iterator should not be nil if the database contains anything.");
}

- (void)testIteratorStartsAtFirstKey
{
	_db[@"b"] = @"2";
	_db[@"a"] = @"1";
	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:_db];
	STAssertEqualObjects([iter key], @"a", @"Iterator should start at the first key.");
	
	STAssertEqualObjects([iter nextKey], @"b", @"Iterator should progress to the second key.");
}

- (void)testIteratorSeek
{
	_db[@"a"] = @"1";
	_db[@"ab"] = @"2";
	_db[@"abc"] = @"3";
	
	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:_db];
	[iter seekToKey:@"ab"];
	
	STAssertEqualObjects([iter key], @"ab", @"Iterator did not seek properly.");
	STAssertEqualObjects([iter valueAsString], @"2", @"Iterator value incorrect.");
	
	STAssertEqualObjects([iter nextKey], @"abc", @"Iterator did not seek properly.");
	STAssertEqualObjects([iter valueAsString], @"3", @"Iterator value incorrect.");
}

- (void)testIteratorSeekToNonExistentKey
{
	_db[@"a"] = @"1";
	_db[@"ab"] = @"2";
	_db[@"abc"] = @"3";
	
	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:_db];
	[iter seekToKey:@"aa"]; // seeking to a key that doesn't exist should jump us to the next possible key.
	
	STAssertEqualObjects([iter key], @"ab", @"Iterator did not seek properly.");
	STAssertEqualObjects([iter valueAsString], @"2", @"Iterator value incorrect.");
	
	STAssertEqualObjects([iter nextKey], @"abc", @"Iterator did not advance properly.");
	STAssertEqualObjects([iter valueAsString], @"3", @"Iterator value incorrect.");
}

- (void)testIteratorStepPastEnd
{
	_db[@"a"] = @"1";
	_db[@"ab"] = @"2";
	_db[@"abc"] = @"3";
	
	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:_db];
	[iter seekToKey:@"ab"];
	[iter nextKey]; // abc
	STAssertNil([iter nextKey], @"Iterator should return nil at end of keys.");
	STAssertNil([iter valueAsData], @"Iterator should return nil at end of keys.");
	STAssertNil([iter valueAsString], @"Iterator should return nil at end of keys.");
}

#pragma mark - Helpers

- (NSData *)largeData
{
	if (!_largeData)
	{
		NSUInteger numberOfBytes = 1024*1024*10; // 10MB
		NSMutableData *data = [NSMutableData dataWithCapacity:numberOfBytes];
		[data setLength:numberOfBytes];
		char *buffer = [data mutableBytes];
		for (NSUInteger i = 0; i < numberOfBytes; i++)
		{
			buffer[i] = i & 0xff;
		}
		_largeData = [data copy];
	}
	return _largeData;
}

- (NSDictionary *)populateWithUUIDsAndReturnDictionary
{
	// Generate random keys and values using UUIDs:
	const int numberOfKeys = 64;
	NSMutableDictionary *keysAndValues = [NSMutableDictionary dictionaryWithCapacity:numberOfKeys];
	for (int i = 0; i < numberOfKeys; i++)
	{
		@autoreleasepool {
			keysAndValues[ [[NSUUID UUID] UUIDString] ] = [[NSUUID UUID] UUIDString];
		}
	}
	
	[keysAndValues enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
		[_db setString:obj forKey:key];
	}];
	
	return keysAndValues;
}



@end
