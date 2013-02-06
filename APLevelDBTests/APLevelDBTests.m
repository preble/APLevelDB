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
	APLevelDB *mDB;
	NSData *mLargeData;
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
	
	mDB = [APLevelDB levelDBWithPath:path error:nil];
}

- (void)tearDown
{
    // Tear-down code here.
	mDB = nil;
    
    [super tearDown];
}

#pragma mark - Tests

- (void)testSetStringForKey
{
	NSString *text = @"Hello";
	NSString *key = @"key";
	[mDB setString:text forKey:key];
	
	STAssertEqualObjects(text, [mDB stringForKey:key], @"Error retrieving string for key.");
}

- (void)testSetDataForKey
{
	// Create some test data using NSKeyedArchiver:
	NSData *data = [NSKeyedArchiver archivedDataWithRootObject:[NSDate date]];
	NSString *key = @"key";
	[mDB setData:data forKey:key];
	
	NSData *fetched = [mDB dataForKey:key];
	STAssertNotNil(fetched, @"Key for data not found.");
	STAssertEqualObjects(data, fetched, @"Fetched data doesn't match original data.");
}

- (void)testNilForUnknownKey
{
	STAssertNil([mDB stringForKey:@"made up key"], @"stringForKey: should return nil if a key doesn't exist");
	STAssertNil([mDB dataForKey:@"another made up key"], @"dataForKey: should return nil if a key doesn't exist");
}

- (void)testRemoveKey
{
	NSString *text = @"Hello";
	NSString *key = @"key";
	[mDB setString:text forKey:key];
	
	STAssertEqualObjects(text, [mDB stringForKey:key], @"stringForKey should have returned the original text");
	
	[mDB removeKey:key];
	
	STAssertNil([mDB stringForKey:key], @"stringForKey should return nil after removal of key");
	STAssertNil([mDB dataForKey:key], @"dataForKey should return nil after removal of key");
}

- (void)testAllKeys
{
	NSDictionary *keysAndValues = [self populateWithUUIDsAndReturnDictionary];

	NSArray *sortedOriginalKeys = [keysAndValues.allKeys sortedArrayUsingSelector:@selector(compare:)];
	STAssertEqualObjects(sortedOriginalKeys, [mDB allKeys], @"");
}

- (void)testEnumeration
{
	NSDictionary *keysAndValues = [self populateWithUUIDsAndReturnDictionary];
	NSArray *sortedOriginalKeys = [keysAndValues.allKeys sortedArrayUsingSelector:@selector(compare:)];
	
	__block NSUInteger keyIndex = 0;
	[mDB enumerateKeys:^(NSString *key, BOOL *stop) {
		STAssertEqualObjects(key, sortedOriginalKeys[keyIndex], @"enumerated key does not match");
		keyIndex++;
	}];
}

- (void)testEnumerationUsingStrings
{
	NSDictionary *keysAndValues = [self populateWithUUIDsAndReturnDictionary];
	NSArray *sortedOriginalKeys = [keysAndValues.allKeys sortedArrayUsingSelector:@selector(compare:)];
	
	__block NSUInteger keyIndex = 0;
	[mDB enumerateKeysAndValuesAsStrings:^(NSString *key, NSString *value, BOOL *stop) {
		
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
	mDB[key] = text;
	
	STAssertEqualObjects(text, mDB[key], @"Error retrieving string for key.");
}

- (void)testSubscriptingNilForUnknownKey
{
	STAssertNil(mDB[@"no such key as this key"], @"Subscripting access should return nil for an unknown key.");
}

- (void)testSubscriptingAccessException
{
	id output;
	STAssertThrowsSpecificNamed(output = mDB[ [NSDate date] ], NSException, NSInvalidArgumentException, @"Subscripting with non-NSString type should raise an NSInvalidArgumentException.");
}
- (void)testSubscriptingAssignmentException
{
	NSData *someData = [NSKeyedArchiver archivedDataWithRootObject:[NSDate date]];
	STAssertThrowsSpecificNamed(mDB[ [NSDate date] ] = @"hello", NSException, NSInvalidArgumentException, @"Subscripting with non-NSString type should raise an NSInvalidArgumentException.");
	STAssertThrowsSpecificNamed(mDB[ @"valid key" ] = [NSDate date], NSException, NSInvalidArgumentException, @"Subscripting with non-NSString type should raise an NSInvalidArgumentException.");
	STAssertNoThrow(mDB[ @"valid key" ] = @"hello", @"Subscripting with non-NSString type should raise an NSInvalidArgumentException.");
	STAssertNoThrow(mDB[ @"valid key" ] = someData, @"Subscripting with non-NSString type should raise an NSInvalidArgumentException.");
}

- (void)testLargeValue
{
	NSString *key = @"key";
	NSData *data = [self largeData];
	
	[mDB setData:data forKey:key];
	STAssertEqualObjects(data, [mDB dataForKey:key], @"Data read from database does not match original.");
}

#pragma mark - Tests - Iterators

- (void)testIteratorNilOnEmptyDatabase
{
	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:mDB];
	STAssertNil(iter, @"Iterator should be nil for an empty database.");
}

- (void)testIteratorNotNilOnPopulatedDatabase
{
	mDB[@"a"] = @"1";
	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:mDB];
	STAssertNotNil(iter, @"Iterator should not be nil if the database contains anything.");
}

- (void)testIteratorStartsAtFirstKey
{
	mDB[@"b"] = @"2";
	mDB[@"a"] = @"1";
	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:mDB];
	STAssertEqualObjects([iter key], @"a", @"Iterator should start at the first key.");
	
	STAssertEqualObjects([iter nextKey], @"b", @"Iterator should progress to the second key.");
}

- (void)testIteratorSeek
{
	mDB[@"a"] = @"1";
	mDB[@"ab"] = @"2";
	mDB[@"abc"] = @"3";
	
	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:mDB];
	[iter seekToKey:@"ab"];
	
	STAssertEqualObjects([iter key], @"ab", @"Iterator did not seek properly.");
	STAssertEqualObjects([iter valueAsString], @"2", @"Iterator value incorrect.");
	
	STAssertEqualObjects([iter nextKey], @"abc", @"Iterator did not seek properly.");
	STAssertEqualObjects([iter valueAsString], @"3", @"Iterator value incorrect.");
}

- (void)testIteratorSeekToNonExistentKey
{
	mDB[@"a"] = @"1";
	mDB[@"ab"] = @"2";
	mDB[@"abc"] = @"3";
	
	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:mDB];
	[iter seekToKey:@"aa"]; // seeking to a key that doesn't exist should jump us to the next possible key.
	
	STAssertEqualObjects([iter key], @"ab", @"Iterator did not seek properly.");
	STAssertEqualObjects([iter valueAsString], @"2", @"Iterator value incorrect.");
	
	STAssertEqualObjects([iter nextKey], @"abc", @"Iterator did not advance properly.");
	STAssertEqualObjects([iter valueAsString], @"3", @"Iterator value incorrect.");
}

- (void)testIteratorStepPastEnd
{
	mDB[@"a"] = @"1";
	mDB[@"ab"] = @"2";
	mDB[@"abc"] = @"3";
	
	APLevelDBIterator *iter = [APLevelDBIterator iteratorWithLevelDB:mDB];
	[iter seekToKey:@"ab"];
	[iter nextKey]; // abc
	STAssertNil([iter nextKey], @"Iterator should return nil at end of keys.");
	STAssertNil([iter valueAsData], @"Iterator should return nil at end of keys.");
	STAssertNil([iter valueAsString], @"Iterator should return nil at end of keys.");
}

#pragma mark - Helpers

- (NSData *)largeData
{
	if (!mLargeData)
	{
		NSUInteger numberOfBytes = 1024*1024*10; // 10MB
		NSMutableData *data = [NSMutableData dataWithCapacity:numberOfBytes];
		[data setLength:numberOfBytes];
		char *buffer = [data mutableBytes];
		for (NSUInteger i = 0; i < numberOfBytes; i++)
		{
			buffer[i] = i & 0xff;
		}
		mLargeData = [data copy];
	}
	return mLargeData;
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
		[mDB setString:obj forKey:key];
	}];
	
	return keysAndValues;
}



@end
