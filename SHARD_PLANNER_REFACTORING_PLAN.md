# ShardPlanner Refactoring Plan

## 🎯 Goal

Extract all inner classes from `ShardPlanner` into separate classes in a new package `com.di.streamnova.util.shardplanner` to make the code more modular and maintainable.

## 📦 Package Structure

```
com.di.streamnova.util.shardplanner/
├── config/
│   ├── DatasetScenarioConfig.java ✅
│   ├── SizeBasedConfig.java ✅
│   ├── CostOptimizationConfig.java ✅
│   └── MachineProfile.java ✅
├── environment/
│   ├── ExecutionEnvironment.java ✅
│   ├── EnvironmentDetector.java ✅
│   └── DataSizeInfo.java ✅
├── optimizer/
│   ├── ScenarioOptimizer.java
│   ├── MachineTypeBasedOptimizer.java
│   ├── MachineTypeAdjuster.java
│   └── CostOptimizer.java
├── validator/
│   ├── MachineTypeResourceValidator.java
│   ├── ConstraintApplier.java
│   └── ShardCountRounder.java
└── calculator/
    ├── UnifiedCalculator.java
    ├── WorkerCountCalculator.java
    ├── MachineProfileProvider.java
    ├── SmallDatasetOptimizer.java
    └── ProfileBasedCalculator.java
```

## ✅ Completed

1. ✅ Package structure created
2. ✅ Configuration classes extracted
3. ✅ Environment classes extracted

## 🔄 In Progress

4. ⏳ Optimization classes
5. ⏳ Validation classes
6. ⏳ Calculation classes
7. ⏳ Update ShardPlanner to use extracted classes

## 📝 Notes

- All extracted classes will be in package `com.di.streamnova.util.shardplanner`
- Classes that need logging will use `@Slf4j` annotation
- All static methods will remain static
- Dependencies between classes will be handled via imports
