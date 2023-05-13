#pragma once

#include "row_validator_generator.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/codegen/type_builder.h>

#include <yt/yt/library/query/row_validator/row_validator_generator.h>

#include <type_traits>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TTypeBuilder<NYT::NYson::TYsonPullParserCursor*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<NYT::NTableClient::TComplexTypeFieldDescriptor*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<NYT::NQueryClient::NDetail::TValidatorPersistentState*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<NYT::NTableClient::TUnversionedRow*>
    : public TTypeBuilder<void*>
{ };


////////////////////////////////////////////////////////////////////////////////

template <>
struct TTypeBuilder<NYT::NQueryClient::NDetail::TFieldId>
{
public:
    typedef TTypeBuilder<NYT::NQueryClient::NDetail::TFieldId*> TParent;
    typedef TTypeBuilder<ui64> TSiblingIndex;

    enum Fields
    {
        Parent,
        SiblingIndex
    };

    static llvm::StructType* Get(llvm::LLVMContext& context)
    {
        if (auto type = llvm::StructType::getTypeByName(context, "TFieldId")) {
            return type;
        }

        auto* structType = llvm::StructType::create(context);

        llvm::Type* elements[] = {
            structType->getPointerTo(),
            TSiblingIndex::Get(context),
        };
        structType->setBody(elements);
        structType->setName("TFieldId");
        return structType;
    }

    static_assert(
        std::is_standard_layout<NYT::NQueryClient::NDetail::TFieldId>::value,
        "TFieldId must be of standart layout type");

    static_assert(
        sizeof(NYT::NQueryClient::NDetail::TFieldId) == 16,
        "TFieldId must be of type {i8*, ui64}");

    static_assert(
        offsetof(NYT::NQueryClient::NDetail::TFieldId, Parent) == 0
            && sizeof(NYT::NQueryClient::NDetail::TFieldId::Parent) == 8,
        "TFieldId must be of type {i8*, ui64}");

    static_assert(
        offsetof(NYT::NQueryClient::NDetail::TFieldId, SiblingIndex) == 8
            && sizeof(NYT::NQueryClient::NDetail::TFieldId::SiblingIndex) == 8,
        "TFieldId must be of type {i8*, ui64}");
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct TTypeBuilder<NYT::NQueryClient::NDetail::TStringView>
{
public:
    typedef TTypeBuilder<const char*> TData;
    typedef TTypeBuilder<ui64> TLength;

    enum Fields
    {
        Data,
        Length
    };

    static llvm::StructType* Get(llvm::LLVMContext& context)
    {
        return llvm::StructType::get(context, {
            TData::Get(context),
            TLength::Get(context)});
    }

    static_assert(
        std::is_standard_layout<NYT::NQueryClient::NDetail::TStringView>::value,
        "TStringView must be of standart layout type");

    static_assert(
        sizeof(NYT::NQueryClient::NDetail::TStringView) == 16,
        "TStringView must be of type {i8*, ui64}");

    static_assert(
        offsetof(NYT::NQueryClient::NDetail::TStringView, Data) == 0
            && sizeof(NYT::NQueryClient::NDetail::TStringView::Data) == 8,
        "TStringView must be of type {i8*, ui64}");

    static_assert(
        offsetof(NYT::NQueryClient::NDetail::TStringView, Length) == 8
            && sizeof(NYT::NQueryClient::NDetail::TStringView::Length) == 8,
        "TStringView must be of type {i8*, ui64}");
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
