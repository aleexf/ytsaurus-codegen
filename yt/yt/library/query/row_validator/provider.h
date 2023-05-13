#pragma once

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/yson/pull_parser.h>

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef void(ILogicalTypeValidatorSignature)(NYT::NYson::TYsonPullParserCursor*, NYT::NTableClient::TComplexTypeFieldDescriptor*);
typedef void(IRowValidatorSignature)(const NTableClient::TUnversionedRow*);

DECLARE_REFCOUNTED_STRUCT(IRowValidatorProvider)

struct IRowValidatorProvider
    : public virtual TRefCounted
{
    virtual TCallback<ILogicalTypeValidatorSignature> Get(NTableClient::TLogicalTypePtr logicalType) = 0;

    virtual TCallback<IRowValidatorSignature> Get(
        const NTableClient::TTableSchemaPtr& schema,
        bool typeAnyAcceptsAllValues,
        bool ignoreRequired = false,
        bool validateAnyIsValidYson = false) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowValidatorProvider)

IRowValidatorProviderPtr CreateRowValidatorProvider();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
