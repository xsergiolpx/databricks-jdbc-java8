package com.databricks.jdbc.api.impl;

import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SqlParameter}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSqlParameter.builder()}.
 */
@Generated(from = "SqlParameter", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableSqlParameter implements SqlParameter {
  private final @Nullable Object value;
  private final ColumnInfoTypeName type;
  private final int cardinal;

  private ImmutableSqlParameter(
      @Nullable Object value,
      ColumnInfoTypeName type,
      int cardinal) {
    this.value = value;
    this.type = type;
    this.cardinal = cardinal;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public @Nullable Object value() {
    return value;
  }

  /**
   * @return The value of the {@code type} attribute
   */
  @Override
  public ColumnInfoTypeName type() {
    return type;
  }

  /**
   * @return The value of the {@code cardinal} attribute
   */
  @Override
  public int cardinal() {
    return cardinal;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SqlParameter#value() value} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSqlParameter withValue(@Nullable Object value) {
    if (this.value == value) return this;
    return new ImmutableSqlParameter(value, this.type, this.cardinal);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SqlParameter#type() type} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for type
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSqlParameter withType(ColumnInfoTypeName value) {
    ColumnInfoTypeName newValue = Objects.requireNonNull(value, "type");
    if (this.type == newValue) return this;
    return new ImmutableSqlParameter(this.value, newValue, this.cardinal);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SqlParameter#cardinal() cardinal} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for cardinal
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSqlParameter withCardinal(int value) {
    if (this.cardinal == value) return this;
    return new ImmutableSqlParameter(this.value, this.type, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSqlParameter} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSqlParameter
        && equalTo(0, (ImmutableSqlParameter) another);
  }

  private boolean equalTo(int synthetic, ImmutableSqlParameter another) {
    return Objects.equals(value, another.value)
        && type.equals(another.type)
        && cardinal == another.cardinal;
  }

  /**
   * Computes a hash code from attributes: {@code value}, {@code type}, {@code cardinal}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Objects.hashCode(value);
    h += (h << 5) + type.hashCode();
    h += (h << 5) + cardinal;
    return h;
  }

  /**
   * Prints the immutable value {@code SqlParameter} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("SqlParameter")
        .omitNullValues()
        .add("value", value)
        .add("type", type)
        .add("cardinal", cardinal)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link SqlParameter} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SqlParameter instance
   */
  public static ImmutableSqlParameter copyOf(SqlParameter instance) {
    if (instance instanceof ImmutableSqlParameter) {
      return (ImmutableSqlParameter) instance;
    }
    return ImmutableSqlParameter.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSqlParameter ImmutableSqlParameter}.
   * <pre>
   * ImmutableSqlParameter.builder()
   *    .value(Object | null) // nullable {@link SqlParameter#value() value}
   *    .type(com.databricks.sdk.service.sql.ColumnInfoTypeName) // required {@link SqlParameter#type() type}
   *    .cardinal(int) // required {@link SqlParameter#cardinal() cardinal}
   *    .build();
   * </pre>
   * @return A new ImmutableSqlParameter builder
   */
  public static ImmutableSqlParameter.Builder builder() {
    return new ImmutableSqlParameter.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSqlParameter ImmutableSqlParameter}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SqlParameter", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_TYPE = 0x1L;
    private static final long INIT_BIT_CARDINAL = 0x2L;
    private long initBits = 0x3L;

    private @Nullable Object value;
    private @Nullable ColumnInfoTypeName type;
    private int cardinal;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code SqlParameter} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(SqlParameter instance) {
      Objects.requireNonNull(instance, "instance");
      @Nullable Object valueValue = instance.value();
      if (valueValue != null) {
        value(valueValue);
      }
      type(instance.type());
      cardinal(instance.cardinal());
      return this;
    }

    /**
     * Initializes the value for the {@link SqlParameter#value() value} attribute.
     * @param value The value for value (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder value(@Nullable Object value) {
      this.value = value;
      return this;
    }

    /**
     * Initializes the value for the {@link SqlParameter#type() type} attribute.
     * @param type The value for type 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder type(ColumnInfoTypeName type) {
      this.type = Objects.requireNonNull(type, "type");
      initBits &= ~INIT_BIT_TYPE;
      return this;
    }

    /**
     * Initializes the value for the {@link SqlParameter#cardinal() cardinal} attribute.
     * @param cardinal The value for cardinal 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder cardinal(int cardinal) {
      this.cardinal = cardinal;
      initBits &= ~INIT_BIT_CARDINAL;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSqlParameter ImmutableSqlParameter}.
     * @return An immutable instance of SqlParameter
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSqlParameter build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableSqlParameter(value, type, cardinal);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TYPE) != 0) attributes.add("type");
      if ((initBits & INIT_BIT_CARDINAL) != 0) attributes.add("cardinal");
      return "Cannot build SqlParameter, some of required attributes are not set " + attributes;
    }
  }
}
