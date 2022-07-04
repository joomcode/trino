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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.joom.trino.plugin.deltalake.parquet.ParquetPageSourceFactory;
import io.airlift.json.JsonCodec;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.ReaderProjectionsAdapter;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.joom.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static com.joom.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.hive.HiveSessionProperties.isParquetUseColumnIndex;
import static java.util.Objects.requireNonNull;

public class DeltaLakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;
    private final int domainCompactionThreshold;
    private final DateTimeZone parquetDateTimeZone;
    private final ExecutorService executorService;
    private final TypeManager typeManager;
    private final JsonCodec<DeltaLakeUpdateResult> updateResultJsonCodec;

    @Inject
    public DeltaLakePageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderConfig parquetReaderConfig,
            DeltaLakeConfig deltaLakeConfig,
            ExecutorService executorService,
            TypeManager typeManager,
            JsonCodec<DeltaLakeUpdateResult> updateResultJsonCodec)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions();
        this.domainCompactionThreshold = requireNonNull(deltaLakeConfig, "deltaLakeConfig is null").getDomainCompactionThreshold();
        this.parquetDateTimeZone = deltaLakeConfig.getParquetDateTimeZone();
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.updateResultJsonCodec = requireNonNull(updateResultJsonCodec, "deleteResultJsonCodec is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        DeltaLakeSplit split = (DeltaLakeSplit) connectorSplit;
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) connectorTable;

        // We reach here when we could not prune the split using file level stats, table predicate
        // and the dynamic filter in the coordinator during split generation. The file level stats
        // in DeltaLakeSplit#filePredicate could help to prune this split when a more selective dynamic filter
        // is available now, without having to access parquet file footer for row-group stats.
        // We avoid sending DeltaLakeSplit#splitPredicate to workers by using table.getPredicate() here.
        TupleDomain<DeltaLakeColumnHandle> filteredSplitPredicate = TupleDomain.intersect(ImmutableList.of(
                table.getNonPartitionConstraint(),
                split.getStatisticsPredicate(),
                dynamicFilter.getCurrentPredicate().transformKeys(DeltaLakeColumnHandle.class::cast)));
        if (filteredSplitPredicate.isNone()) {
            return new EmptyPageSource();
        }

        List<DeltaLakeColumnHandle> deltaLakeColumns = columns.stream()
                .map(DeltaLakeColumnHandle.class::cast)
                .collect(toImmutableList());

        Map<String, Optional<String>> partitionKeys = split.getPartitionKeys();

        List<DeltaLakeColumnHandle> regularColumns = deltaLakeColumns.stream()
                .filter(column -> column.getColumnType() == REGULAR)
                .collect(toImmutableList());

        List<HiveColumnHandle> columnsFromParquet = regularColumns.stream()
                .map(DeltaLakeColumnHandle::toHiveColumnHandle)
                .collect(toImmutableList());

        Path path = new Path(split.getPath());
        HdfsContext hdfsContext = new HdfsContext(session);
        TupleDomain<HiveColumnHandle> parquetPredicate = getParquetTupleDomain(filteredSplitPredicate.simplify(domainCompactionThreshold));

        if (table.getWriteType().isPresent()) {
            return new DeltaLakeUpdatablePageSource(
                    table,
                    deltaLakeColumns,
                    partitionKeys,
                    split.getPath(),
                    split.getFileSize(),
                    split.getFileModifiedTime(),
                    session,
                    executorService,
                    hdfsEnvironment,
                    hdfsContext,
                    parquetDateTimeZone,
                    parquetReaderOptions,
                    parquetPredicate,
                    typeManager,
                    updateResultJsonCodec);
        }

        ReaderPageSource reader = createParquetPageSourceReader(
                session,
                path,
                split,
                columnsFromParquet,
                parquetPredicate,
                hdfsContext);

        Optional<ReaderProjectionsAdapter> adapter = Optional.empty();
        Optional<ReaderColumns> readerProjections = reader.getReaderColumns();

        if (readerProjections.isPresent()) {
            adapter = Optional.of(projectionsAdapter(columnsFromParquet, readerProjections.get()));
        }

        return new DeltaLakePageSource(
                deltaLakeColumns,
                partitionKeys,
                adapter,
                reader.get(),
                split.getPath(),
                split.getFileSize(),
                split.getFileModifiedTime());
    }

    private ReaderPageSource createParquetPageSourceReader(
            ConnectorSession session,
            Path path,
            DeltaLakeSplit split,
            List<HiveColumnHandle> columnsFromParquet,
            TupleDomain<HiveColumnHandle> parquetPredicate,
            HdfsContext hdfsContext)
    {
        return ParquetPageSourceFactory.createPageSource(
                path,
                split.getStart(),
                split.getLength(),
                split.getFileSize(),
                columnsFromParquet,
                parquetPredicate,
                true,
                hdfsEnvironment,
                hdfsEnvironment.getConfiguration(hdfsContext, path),
                session.getIdentity(),
                parquetDateTimeZone,
                fileFormatDataSourceStats,
                parquetReaderOptions
                        .withMaxReadBlockSize(getParquetMaxReadBlockSize(session))
                        .withUseColumnIndex(isParquetUseColumnIndex(session)));
    }

    private static ReaderProjectionsAdapter projectionsAdapter(List<HiveColumnHandle> expectedColumns, ReaderColumns readColumns)
    {
        return new ReaderProjectionsAdapter(
                expectedColumns.stream().map(ColumnHandle.class::cast).collect(toImmutableList()),
                readColumns,
                column -> ((HiveColumnHandle) column).getType(),
                DeltaLakePageSourceProvider::getProjection);
    }

    static List<Integer> getProjection(ColumnHandle expected, ColumnHandle read)
    {
        // todo: replace hive columns projection info to deltalake to reduce code duplication
        HiveColumnHandle expectedColumn = (HiveColumnHandle) expected;
        HiveColumnHandle readColumn = (HiveColumnHandle) read;

        checkArgument(expectedColumn.getBaseColumn().equals(readColumn.getBaseColumn()), "reader column is not valid for expected column");

        List<Integer> expectedDereferences = expectedColumn.getHiveColumnProjectionInfo()
                .map(HiveColumnProjectionInfo::getDereferenceIndices)
                .orElse(ImmutableList.of());

        List<Integer> readerDereferences = readColumn.getHiveColumnProjectionInfo()
                .map(HiveColumnProjectionInfo::getDereferenceIndices)
                .orElse(ImmutableList.of());

        checkArgument(readerDereferences.size() <= expectedDereferences.size(), "Field returned by the reader should include expected field");
        checkArgument(expectedDereferences.subList(0, readerDereferences.size()).equals(readerDereferences), "Field returned by the reader should be a prefix of expected field");

        return expectedDereferences.subList(readerDereferences.size(), expectedDereferences.size());
    }

    private static TupleDomain<HiveColumnHandle> getParquetTupleDomain(TupleDomain<DeltaLakeColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<HiveColumnHandle, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().get().forEach((columnHandle, domain) -> {
            String baseType = columnHandle.getType().getTypeSignature().getBase();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!baseType.equals(StandardTypes.MAP) && !baseType.equals(StandardTypes.ARRAY) && !baseType.equals(StandardTypes.ROW)) {
                HiveColumnHandle hiveColumnHandle = columnHandle.toHiveColumnHandle();
                predicate.put(hiveColumnHandle, domain);
            }
        });
        return TupleDomain.withColumnDomains(predicate.buildOrThrow());
    }
}
