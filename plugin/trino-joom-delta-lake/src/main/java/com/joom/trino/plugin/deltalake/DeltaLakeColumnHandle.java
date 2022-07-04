/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.joom.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;
import java.util.Optional;

import static com.joom.trino.plugin.deltalake.DeltaHiveTypeTranslator.toHiveType;
import static com.joom.trino.plugin.deltalake.DeltaLakeColumnType.SYNTHESIZED;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DeltaLakeColumnHandle
        implements ColumnHandle
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DeltaLakeColumnHandle.class).instanceSize();

    public static final String ROW_ID_COLUMN_NAME = "$row_id";
    public static final Type ROW_ID_COLUMN_TYPE = BIGINT;

    public static final String PATH_COLUMN_NAME = "$path";
    public static final Type PATH_TYPE = VARCHAR;

    public static final String FILE_SIZE_COLUMN_NAME = "$file_size";
    public static final Type FILE_SIZE_TYPE = BIGINT;

    public static final String FILE_MODIFIED_TIME_COLUMN_NAME = "$file_modified_time";
    public static final Type FILE_MODIFIED_TIME_TYPE = TIMESTAMP_WITH_TIME_ZONE;

    private final String baseColumnName;
    private final Type baseType;
    private final DeltaLakeColumnType columnType;
    private final Optional<DeltaLakeColumnProjectionInfo> deltaLakeColumnProjectionInfo;
    private final String name;

    @JsonCreator
    public DeltaLakeColumnHandle(
            @JsonProperty("baseColumnName") String baseColumnName,
            @JsonProperty("baseType") Type baseType,
            @JsonProperty("columnType") DeltaLakeColumnType columnType,
            @JsonProperty("deltaLakeColumnProjectionInfo") Optional<DeltaLakeColumnProjectionInfo> deltaLakeColumnProjectionInfo)
    {
        this.baseColumnName = requireNonNull(baseColumnName, "baseName is null");
        this.baseType = requireNonNull(baseType, "baseType is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.deltaLakeColumnProjectionInfo = requireNonNull(deltaLakeColumnProjectionInfo, "deltaLakeColumnProjectionInfo is null");
        this.name = this.baseColumnName + deltaLakeColumnProjectionInfo
                .map(DeltaLakeColumnProjectionInfo::getPartialName).orElse("");
    }

    @JsonProperty
    public String getBaseColumnName()
    {
        return baseColumnName;
    }

    @JsonProperty
    public Type getBaseType()
    {
        return baseType;
    }

    @JsonProperty
    public DeltaLakeColumnType getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public Optional<DeltaLakeColumnProjectionInfo> getDeltaLakeColumnProjectionInfo()
    {
        return deltaLakeColumnProjectionInfo;
    }

    public DeltaLakeColumnHandle getBaseColumn()
    {
        return isBaseColumn() ? this : createBaseColumn(baseColumnName, baseType, columnType);
    }

    public boolean isBaseColumn()
    {
        return deltaLakeColumnProjectionInfo.isEmpty();
    }

    public static DeltaLakeColumnHandle createBaseColumn(
            String topLevelColumnName,
            Type type,
            DeltaLakeColumnType columnType)
    {
        return new DeltaLakeColumnHandle(topLevelColumnName, type, columnType, Optional.empty());
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return deltaLakeColumnProjectionInfo.map(DeltaLakeColumnProjectionInfo::getType).orElse(baseType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DeltaLakeColumnHandle other = (DeltaLakeColumnHandle) obj;
        return Objects.equals(this.baseColumnName, other.baseColumnName) &&
                Objects.equals(this.baseType, other.baseType) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.deltaLakeColumnProjectionInfo, other.deltaLakeColumnProjectionInfo) &&
                this.columnType == other.columnType;
    }

    public Optional<HiveColumnProjectionInfo> getHiveColumnProjectionInfo()
    {
        return deltaLakeColumnProjectionInfo.flatMap(info -> Optional.of(new HiveColumnProjectionInfo(
                info.getDereferenceIndices(),
                info.getDereferenceNames(),
                info.getHiveType(),
                info.getType())));
    }

    public long getRetainedSizeInBytes()
    {
        // type is not accounted for as the instances are cached (by TypeRegistry) and shared
        return INSTANCE_SIZE + estimatedSizeOf(name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, baseType, deltaLakeColumnProjectionInfo, columnType);
    }

    @Override
    public String toString()
    {
        return name + ":" + getType().getDisplayName() + ":" + columnType;
    }

    public HiveColumnHandle toHiveColumnHandle()
    {
        return new HiveColumnHandle(
                baseColumnName,
                0, // hiveColumnIndex; we provide fake value because we always find columns by name
                toHiveType(baseType),
                baseType,
                deltaLakeColumnProjectionInfo.flatMap(DeltaLakeColumnProjectionInfo::getHiveColumnProjectionInfo),
                columnType.toHiveColumnType(),
                Optional.empty());
    }

    public HiveType getHiveType()
    {
        return toHiveType(getType());
    }

    public static DeltaLakeColumnHandle pathColumnHandle()
    {
        return new DeltaLakeColumnHandle(PATH_COLUMN_NAME, PATH_TYPE, SYNTHESIZED, Optional.empty());
    }

    public static DeltaLakeColumnHandle fileSizeColumnHandle()
    {
        return new DeltaLakeColumnHandle(FILE_SIZE_COLUMN_NAME, FILE_SIZE_TYPE, SYNTHESIZED, Optional.empty());
    }

    public static DeltaLakeColumnHandle fileModifiedTimeColumnHandle()
    {
        return new DeltaLakeColumnHandle(FILE_MODIFIED_TIME_COLUMN_NAME, FILE_MODIFIED_TIME_TYPE, SYNTHESIZED, Optional.empty());
    }
}
