package com.databricks.jdbc.api.impl.batch;

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
 * Immutable implementation of {@link BatchCommand}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBatchCommand.builder()}.
 */
@Generated(from = "BatchCommand", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableBatchCommand implements BatchCommand {
  private final String sql;

  private ImmutableBatchCommand(String sql) {
    this.sql = sql;
  }

  /**
   *Returns the SQL command string. 
   */
  @Override
  public String getSql() {
    return sql;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BatchCommand#getSql() sql} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for sql
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBatchCommand withSql(String value) {
    String newValue = Objects.requireNonNull(value, "sql");
    if (this.sql.equals(newValue)) return this;
    return new ImmutableBatchCommand(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBatchCommand} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBatchCommand
        && equalTo(0, (ImmutableBatchCommand) another);
  }

  private boolean equalTo(int synthetic, ImmutableBatchCommand another) {
    return sql.equals(another.sql);
  }

  /**
   * Computes a hash code from attributes: {@code sql}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + sql.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code BatchCommand} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("BatchCommand")
        .omitNullValues()
        .add("sql", sql)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link BatchCommand} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BatchCommand instance
   */
  public static ImmutableBatchCommand copyOf(BatchCommand instance) {
    if (instance instanceof ImmutableBatchCommand) {
      return (ImmutableBatchCommand) instance;
    }
    return ImmutableBatchCommand.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBatchCommand ImmutableBatchCommand}.
   * <pre>
   * ImmutableBatchCommand.builder()
   *    .sql(String) // required {@link BatchCommand#getSql() sql}
   *    .build();
   * </pre>
   * @return A new ImmutableBatchCommand builder
   */
  public static ImmutableBatchCommand.Builder builder() {
    return new ImmutableBatchCommand.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBatchCommand ImmutableBatchCommand}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BatchCommand", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_SQL = 0x1L;
    private long initBits = 0x1L;

    private @Nullable String sql;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code BatchCommand} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(BatchCommand instance) {
      Objects.requireNonNull(instance, "instance");
      sql(instance.getSql());
      return this;
    }

    /**
     * Initializes the value for the {@link BatchCommand#getSql() sql} attribute.
     * @param sql The value for sql 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder sql(String sql) {
      this.sql = Objects.requireNonNull(sql, "sql");
      initBits &= ~INIT_BIT_SQL;
      return this;
    }

    /**
     * Builds a new {@link ImmutableBatchCommand ImmutableBatchCommand}.
     * @return An immutable instance of BatchCommand
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBatchCommand build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableBatchCommand(sql);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_SQL) != 0) attributes.add("sql");
      return "Cannot build BatchCommand, some of required attributes are not set " + attributes;
    }
  }
}
