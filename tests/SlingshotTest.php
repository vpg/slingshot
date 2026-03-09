<?php
namespace vpg\slingshot\Tests;

use PHPUnit_Framework_TestCase;
use ReflectionClass;
use ReflectionMethod;
use vpg\slingshot\Slingshot;

/**
 * Unit tests for Slingshot (validation and helpers via reflection)
 */
class SlingshotTest extends PHPUnit_Framework_TestCase
{
    /**
     * @return Slingshot instance created without calling constructor (no ES/Phalcon deps)
     */
    private function createSlingshotWithoutConstructor()
    {
        $rc = new ReflectionClass(Slingshot::class);
        return $rc->newInstanceWithoutConstructor();
    }

    /**
     * Invoke a private method on Slingshot instance
     *
     * @param Slingshot $instance
     * @param string    $methodName
     * @param array     $args
     * @return mixed
     */
    private function invokePrivate(Slingshot $instance, $methodName, array $args = [])
    {
        $rm = new ReflectionMethod(Slingshot::class, $methodName);
        $rm->setAccessible(true);
        return $rm->invokeArgs($instance, $args);
    }

    public function testSlingshotClassExists()
    {
        $this->assertTrue(class_exists(Slingshot::class));
    }

    public function testIsMigrationConfValidReturnsFalseWhenFromIndexMissing()
    {
        $s = $this->createSlingshotWithoutConstructor();
        $migrationHash = [
            'from' => ['type' => 't', 'index' => ''],
            'to'   => ['index' => 'ti', 'type' => 't'],
        ];
        $this->assertFalse($this->invokePrivate($s, 'isMigrationConfValid', [$migrationHash]));
    }

    public function testIsMigrationConfValidReturnsFalseWhenFromTypeMissing()
    {
        $s = $this->createSlingshotWithoutConstructor();
        $migrationHash = [
            'from' => ['index' => 'fi', 'type' => ''],
            'to'   => ['index' => 'ti', 'type' => 't'],
        ];
        $this->assertFalse($this->invokePrivate($s, 'isMigrationConfValid', [$migrationHash]));
    }

    public function testIsMigrationConfValidReturnsFalseWhenToIndexMissing()
    {
        $s = $this->createSlingshotWithoutConstructor();
        $migrationHash = [
            'from' => ['index' => 'fi', 'type' => 't'],
            'to'   => ['index' => '', 'type' => 't'],
        ];
        $this->assertFalse($this->invokePrivate($s, 'isMigrationConfValid', [$migrationHash]));
    }

    public function testIsMigrationConfValidReturnsFalseWhenToTypeMissing()
    {
        $s = $this->createSlingshotWithoutConstructor();
        $migrationHash = [
            'from' => ['index' => 'fi', 'type' => 't'],
            'to'   => ['index' => 'ti', 'type' => ''],
        ];
        $this->assertFalse($this->invokePrivate($s, 'isMigrationConfValid', [$migrationHash]));
    }

    public function testIsMigrationConfValidReturnsTrueWhenConfValid()
    {
        $s = $this->createSlingshotWithoutConstructor();
        $migrationHash = [
            'from' => ['index' => 'fromIndex', 'type' => 'fromType'],
            'to'   => ['index' => 'toIndex', 'type' => 'toType'],
        ];
        $this->assertTrue($this->invokePrivate($s, 'isMigrationConfValid', [$migrationHash]));
    }

    public function testIsHostsConfValidReturnsFalseWhenFromMissing()
    {
        $s = $this->createSlingshotWithoutConstructor();
        $this->assertFalse($this->invokePrivate($s, 'isHostsConfValid', [[]]));
        $this->assertFalse($this->invokePrivate($s, 'isHostsConfValid', [['to' => 'localhost:9200']]));
    }

    public function testIsHostsConfValidReturnsTrueWhenFromPresent()
    {
        $s = $this->createSlingshotWithoutConstructor();
        $this->assertTrue($this->invokePrivate($s, 'isHostsConfValid', [['from' => 'localhost:9200']]));
    }

    public function testShouldSplittDocReturnsTrueForNumericKeysOnly()
    {
        $s = $this->createSlingshotWithoutConstructor();
        $this->assertTrue($this->invokePrivate($s, 'shouldSplittDoc', [[0 => 'a', 1 => 'b']]));
        $this->assertTrue($this->invokePrivate($s, 'shouldSplittDoc', [['0' => 'a']]));
    }

    public function testShouldSplittDocReturnsFalseWhenNonNumericKeyPresent()
    {
        $s = $this->createSlingshotWithoutConstructor();
        $this->assertFalse($this->invokePrivate($s, 'shouldSplittDoc', [['id' => 1]]));
        $this->assertFalse($this->invokePrivate($s, 'shouldSplittDoc', [['0' => 'a', 'foo' => 'b']]));
    }

    public function testConstructorThrowsOnInvalidMigrationHash()
    {
        $this->setExpectedException('Exception', 'Wrong migrationHash paramater');
        new Slingshot(
            ['from' => 'localhost:9200'],
            ['from' => ['index' => 'a'], 'to' => ['index' => 'b']] // missing type
        );
    }

    public function testConstructorThrowsOnInvalidHostsConf()
    {
        $this->setExpectedException('Exception', 'Wrong hosts conf paramater');
        new Slingshot(
            [],
            [
                'from' => ['index' => 'fi', 'type' => 't'],
                'to'   => ['index' => 'ti', 'type' => 't'],
            ]
        );
    }
}
