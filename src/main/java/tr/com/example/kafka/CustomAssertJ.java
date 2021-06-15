//package tr.com.havelsan.kkss.kafka;
//
//import java.util.*;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//import java.util.Comparator;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.TreeMap;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//import org.assertj.core.annotations.Beta;
//import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
//import org.assertj.core.description.Description;
//import org.assertj.core.error.ShouldNotBeNull;
//import org.assertj.core.extractor.Extractors;
//import org.assertj.core.groups.Tuple;
//import org.assertj.core.internal.TypeComparators;
//import org.assertj.core.util.CheckReturnValue;
//
//public class CustomAssertJ<SELF extends AbstractObjectAssert<SELF, ACTUAL>, ACTUAL> extends AbstractAssert<SELF, ACTUAL> {
//
//
//    private Map<String, Comparator<?>> comparatorByPropertyOrField = new TreeMap();
//    private TypeComparators comparatorByType;
//
//    public AbstractObjectAssert(ACTUAL actual, Class<?> selfType) {
//        super(actual, selfType);
//    }
//
//    @CheckReturnValue
//    public SELF as(Description description) {
//        return (AbstractObjectAssert) super.as(description);
//    }
//
//    @CheckReturnValue
//    public SELF as(String description, Object... args) {
//        return (AbstractObjectAssert) super.as(description, args);
//    }
//
//    public SELF isEqualToIgnoringNullFields(Object other) {
//        this.objects.assertIsEqualToIgnoringNullFields(this.info, this.actual, other, this.comparatorByPropertyOrField, this.getComparatorsByType());
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    public SELF isEqualToComparingOnlyGivenFields(Object other, String... propertiesOrFieldsUsedInComparison) {
//        this.objects.assertIsEqualToComparingOnlyGivenFields(this.info, this.actual, other, this.comparatorByPropertyOrField, this.getComparatorsByType(), propertiesOrFieldsUsedInComparison);
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    public SELF isEqualToIgnoringGivenFields(Object other, String... propertiesOrFieldsToIgnore) {
//        this.objects.assertIsEqualToIgnoringGivenFields(this.info, this.actual, other, this.comparatorByPropertyOrField, this.getComparatorsByType(), propertiesOrFieldsToIgnore);
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    public SELF hasNoNullFieldsOrProperties() {
//        this.objects.assertHasNoNullFieldsOrPropertiesExcept(this.info, this.actual, new String[0]);
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    public SELF hasAllNullFieldsOrProperties() {
//        this.objects.assertHasAllNullFieldsOrPropertiesExcept(this.info, this.actual, new String[0]);
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    public SELF hasNoNullFieldsOrPropertiesExcept(String... propertiesOrFieldsToIgnore) {
//        this.objects.assertHasNoNullFieldsOrPropertiesExcept(this.info, this.actual, propertiesOrFieldsToIgnore);
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    public SELF hasAllNullFieldsOrPropertiesExcept(String... propertiesOrFieldsToIgnore) {
//        this.objects.assertHasAllNullFieldsOrPropertiesExcept(this.info, this.actual, propertiesOrFieldsToIgnore);
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    public SELF isEqualToComparingFieldByField(Object other) {
//        this.objects.assertIsEqualToIgnoringGivenFields(this.info, this.actual, other, this.comparatorByPropertyOrField, this.getComparatorsByType(), new String[0]);
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    protected TypeComparators getComparatorsByType() {
//        if (this.comparatorByType == null) {
//            this.comparatorByType = TypeComparators.defaultTypeComparators();
//        }
//
//        return this.comparatorByType;
//    }
//
//    @CheckReturnValue
//    public <T> SELF usingComparatorForFields(Comparator<T> comparator, String... propertiesOrFields) {
//        String[] var3 = propertiesOrFields;
//        int var4 = propertiesOrFields.length;
//
//        for (int var5 = 0; var5 < var4; ++var5) {
//            String propertyOrField = var3[var5];
//            this.comparatorByPropertyOrField.put(propertyOrField, comparator);
//        }
//
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    @CheckReturnValue
//    public <T> SELF usingComparatorForType(Comparator<? super T> comparator, Class<T> type) {
//        this.getComparatorsByType().put(type, comparator);
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    public SELF hasFieldOrProperty(String name) {
//        this.objects.assertHasFieldOrProperty(this.info, this.actual, name);
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    public SELF hasFieldOrPropertyWithValue(String name, Object value) {
//        this.objects.assertHasFieldOrPropertyWithValue(this.info, this.actual, name, value);
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    @CheckReturnValue
//    public AbstractListAssert<?, List<?>, Object, ObjectAssert<Object>> extracting(String... propertiesOrFields) {
//        Tuple values = (Tuple) Extractors.byName(propertiesOrFields).apply(this.actual);
//        String extractedPropertiesOrFieldsDescription = Extractors.extractedDescriptionOf(propertiesOrFields);
//        String description = Description.mostRelevantDescription(this.info.description(), extractedPropertiesOrFieldsDescription);
//        return this.newListAssertInstance(values.toList()).as(description, new Object[0]);
//    }
//
//    @CheckReturnValue
//    public AbstractObjectAssert<?, ?> extracting(String propertyOrField) {
//        Object value = Extractors.byName(propertyOrField).apply(this.actual);
//        String extractedPropertyOrFieldDescription = Extractors.extractedDescriptionOf(new String[]{propertyOrField});
//        String description = Description.mostRelevantDescription(this.info.description(), extractedPropertyOrFieldDescription);
//        return this.newObjectAssert(value).as(description);
//    }
//
//    @CheckReturnValue
//    public AbstractListAssert<?, List<?>, Object, ObjectAssert<Object>> extracting(Function<? super ACTUAL, ?>... extractors) {
//        Objects.requireNonNull(extractors, ShouldNotBeNull.shouldNotBeNull("extractors").create());
//        List<Object> values = (List) Stream.of(extractors).map((extractor) -> {
//            return extractor.apply(this.actual);
//        }).collect(Collectors.toList());
//        return (AbstractListAssert) this.newListAssertInstance(values).withAssertionState(this.myself);
//    }
//
//    @CheckReturnValue
//    public <T> AbstractObjectAssert<?, T> extracting(Function<? super ACTUAL, T> extractor) {
//        Objects.requireNonNull(extractor, ShouldNotBeNull.shouldNotBeNull("extractor").create());
//        T extractedValue = extractor.apply(this.actual);
//        return this.newObjectAssert(extractedValue).withAssertionState(this.myself);
//    }
//
//
//    public <T> SELF returns(T expected, Function<ACTUAL, T> from) {
//        Objects.requireNonNull(from, "The given getter method/Function must not be null");
//        this.objects.assertEqual(this.info, from.apply(this.actual), expected);
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    @Beta
//    public RecursiveComparisonAssert<?> usingRecursiveComparison() {
//        return this.usingRecursiveComparison(new RecursiveComparisonConfiguration());
//    }
//
//    @Beta
//    public RecursiveComparisonAssert<?> usingRecursiveComparison(RecursiveComparisonConfiguration recursiveComparisonConfiguration) {
//        return ((RecursiveComparisonAssert) (new RecursiveComparisonAssert(this.actual, recursiveComparisonConfiguration)).withAssertionState(this.myself)).withTypeComparators(this.comparatorByType);
//    }
//
//    protected <T> AbstractObjectAssert<?, T> newObjectAssert(T objectUnderTest) {
//        return new ObjectAssert(objectUnderTest);
//    }
//
//    SELF withAssertionState(AbstractAssert assertInstance) {
//        if (assertInstance instanceof AbstractObjectAssert) {
//            AbstractObjectAssert objectAssert = (AbstractObjectAssert) assertInstance;
//            return ((AbstractObjectAssert) super.withAssertionState(assertInstance)).withTypeComparator(objectAssert.comparatorByType).withComparatorByPropertyOrField(objectAssert.comparatorByPropertyOrField);
//        } else {
//            return (AbstractObjectAssert) super.withAssertionState(assertInstance);
//        }
//    }
//
//    SELF withTypeComparator(TypeComparators comparatorsByType) {
//        this.comparatorByType = comparatorsByType;
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    public SELF isEqualToComparingFieldByFieldRecursively(Object other) {
//        this.objects.assertIsEqualToComparingFieldByFieldRecursively(this.info, this.actual, other, this.comparatorByPropertyOrField, this.getComparatorsByType());
//        return (AbstractObjectAssert) this.myself;
//    }
//
//    SELF withComparatorByPropertyOrField(Map<String, Comparator<?>> comparatorsToPropaget) {
//        this.comparatorByPropertyOrField = comparatorsToPropaget;
//        return (AbstractObjectAssert) this.myself;
//    }
//
//
//}
