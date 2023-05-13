#include "row_validator_generator.h"

#include "llvm_types.h"
#include "provider.h"
#include "helpers.h"

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/llvm_types.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/library/codegen/module.h>
#include <yt/yt/library/codegen/control_flow.h>
#include <yt/yt/library/codegen/builder_base.h>
#include <yt/yt/library/codegen/llvm_migrate_helpers.h>

#include <yt/yt/core/actions/callback.h>

#include <util/charset/utf8.h>
#include <util/generic/adaptor.h>
#include <util/stream/mem.h>

#include <llvm/ADT/Twine.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Type.h>

namespace NYT::NQueryClient {

using namespace NCodegen;
using namespace NTableClient;
using namespace NYson;
using namespace NDetail;
using namespace llvm;

////////////////////////////////////////////////////////////////////////////////

static void RegisterValidatorRoutinesImpl(TRoutineRegistry* registry)
{
    registry->RegisterRoutine("ThrowParseError", ThrowParseError<NTableClient::EErrorCode>);
    registry->RegisterRoutine("ThrowUnexpectedYsonToken", ThrowUnexpectedYsonToken);
    registry->RegisterRoutine("ThrowOutOfRange_Int64", ThrowOutOfRange<i64>);
    registry->RegisterRoutine("ThrowOutOfRange_Uint64", ThrowOutOfRange<ui64>);
    registry->RegisterRoutine("ThrowOutOfRange_Double", ThrowOutOfRange<double>);
    registry->RegisterRoutine("ThrowBadVariantAlternative", ThrowBadVariantAlternative);
    registry->RegisterRoutine("ThrowMissingRequiredField", ThrowMissingRequiredField);
    registry->RegisterRoutine("ThrowNullRequiredColumn", ThrowNullRequiredColumn);
    registry->RegisterRoutine("ThrowBadColumnValue", ThrowBadColumnValue);
    registry->RegisterRoutine("Cursor_GetCurrent_Type", Cursor_GetCurrent_Type);
    registry->RegisterRoutine("Cursor_Next", Cursor_Next);
    registry->RegisterRoutine("Cursor_SkipComplexValue", Cursor_SkipComplexValue);
    registry->RegisterRoutine("Cursor_UncheckedAsBoolean", Cursor_UncheckedAsBoolean);
    registry->RegisterRoutine("Cursor_UncheckedAsInt64", Cursor_UncheckedAsInt64);
    registry->RegisterRoutine("Cursor_UncheckedAsUint64", Cursor_UncheckedAsUint64);
    registry->RegisterRoutine("Cursor_UncheckedAsDouble", Cursor_UncheckedAsDouble);
    registry->RegisterRoutine("Cursor_UncheckedAsStringView", Cursor_UncheckedAsStringView);
    registry->RegisterRoutine("ValidateDecimalValue", ValidateDecimalValue);
    registry->RegisterRoutine("ValidateYsonValue", ValidateYsonValue);
    registry->RegisterRoutine("ValidateJsonValue", ValidateJsonValue);
    registry->RegisterRoutine("InitYsonParser", InitYsonParser);
    registry->RegisterRoutine("UTF8Detect", static_cast<EUTF8Detect(*)(const char*, size_t)>(UTF8Detect));
}

static TRoutineRegistry* GetValidatorRoutineRegistry()
{
    static TRoutineRegistry registry;
    static std::once_flag onceFlag;
    std::call_once(onceFlag, &RegisterValidatorRoutinesImpl, &registry);
    return &registry;
}

////////////////////////////////////////////////////////////////////////////////

TComplexTypeFieldDescriptor NDetail::TFieldId::GetDescriptor(const TComplexTypeFieldDescriptor &root) const
{
    std::vector<int> path;
    const auto* current = this;
    while (current->Parent != nullptr) {
        path.push_back(current->SiblingIndex);
        current = current->Parent;
    }

    auto descriptor = root;
    for (const auto& childIndex : Reversed(path)) {
        const auto& type = descriptor.GetType();
        switch (type->GetMetatype()) {
            case ELogicalMetatype::Simple:
            case ELogicalMetatype::Decimal:
                return descriptor;

            case ELogicalMetatype::Optional:
                descriptor = descriptor.OptionalElement();
                continue;
            case ELogicalMetatype::List:
                descriptor = descriptor.ListElement();
                continue;
            case ELogicalMetatype::Struct:
                descriptor = descriptor.StructField(childIndex);
                continue;
            case ELogicalMetatype::Tuple:
                descriptor = descriptor.TupleElement(childIndex);
                continue;
            case ELogicalMetatype::VariantStruct:
                descriptor = descriptor.VariantStructField(childIndex);
                continue;
            case ELogicalMetatype::VariantTuple:
                descriptor = descriptor.VariantTupleElement(childIndex);
                continue;
            case ELogicalMetatype::Dict:
                switch (childIndex) {
                    case 0:
                        descriptor = descriptor.DictKey();
                        continue;
                    case 1:
                        descriptor = descriptor.DictValue();
                        continue;
                }
                break;
            case ELogicalMetatype::Tagged:
                descriptor = descriptor.TaggedElement();
                continue;
        }
        YT_ABORT();
    }
    return descriptor;
}

////////////////////////////////////////////////////////////////////////////////

TValidatorPersistentState::TValidatorPersistentState()
    : MemoryInput("#")
    , Parser(TYsonPullParser(&MemoryInput, EYsonType::Node))
    , Cursor(&*Parser)
{ }

////////////////////////////////////////////////////////////////////////////////

class TLogicalTypeValidatorBuilder
    : public TIRBuilderBase
{
public:
    explicit TLogicalTypeValidatorBuilder(TCGModulePtr module);

    void GenerateValidator(const TString& functionName, const TLogicalTypePtr& type);

protected:
    class TFieldIdBuilder;
    class TStringViewAccessor;

    void BuildValidateLogicalType(const TLogicalTypePtr& type, Value* fieldIdPtr);

    void BuildValidateOptionalType(const TOptionalLogicalType& type, Value* fieldIdPtr);

    void BuildValidateListType(const TListLogicalType& type, Value* fieldIdPtr);

    void BuildValidateStructType(const TStructLogicalType& type, Value* fieldIdPtr);

    void BuildValidateTupleType(const TTupleLogicalType& type, Value* fieldIdPtr);

    template <class TLogicalType>
        requires std::is_same_v<TLogicalType, TVariantTupleLogicalType> ||
                 std::is_same_v<TLogicalType, TVariantStructLogicalType>
    void BuildValidateVariantType(const TLogicalType& type, Value* fieldIdPtr);

    void BuildValidateDictType(const TDictLogicalType& type, Value* fieldIdPtr);

    void BuildValidateTaggedType(const TTaggedLogicalType& type, Value* fieldIdPtr);

    void BuildValidateDecimalType(const TDecimalLogicalType& type, Value* fieldIdPtr);

    void BuildValidateSimpleType(ESimpleLogicalValueType type, Value* fieldIdPtr);

    template <EYsonItemType itemType>
    void BuildValidateSimpleLogicalType(ESimpleLogicalValueType type, Value* value, Value* fieldIdPtr);

    template <>
    void BuildValidateSimpleLogicalType<EYsonItemType::BooleanValue>(ESimpleLogicalValueType type, Value* value, Value* fieldIdPtr);

    template <>
    void BuildValidateSimpleLogicalType<EYsonItemType::Int64Value>(ESimpleLogicalValueType type, Value* value, Value* fieldIdPtr);

    template <>
    void BuildValidateSimpleLogicalType<EYsonItemType::Uint64Value>(ESimpleLogicalValueType type, Value* value, Value* fieldIdPtr);

    template <>
    void BuildValidateSimpleLogicalType<EYsonItemType::DoubleValue>(ESimpleLogicalValueType type, Value* value, Value* fieldIdPtr);

    template <>
    void BuildValidateSimpleLogicalType<EYsonItemType::StringValue>(ESimpleLogicalValueType type, Value* value, Value* fieldIdPtr);

    template <EYsonItemType itemType>
    void BuildValidateNumericRange(Value* value, Value* min, Value* max, Value* fieldIdPtr);

    void BuildValidateYsonTokenType(EYsonItemType type, Value* fieldIdPtr);

    void BuildValidateYsonTokenTypeNotEqual(EYsonItemType type, Value* fieldIdPtr);

    void BuildValidateYsonTokenTypeImpl(CmpInst::Predicate failIfPredicate, EYsonItemType type, Value* fieldIdPtr);

    template <class TErrorCode>
    void CreateThrowParseError(TErrorCode errorCode, TStringBuf errorMessage, Value* fieldIdPtr);

protected:
    Value* RootDescriptorPtr_;
    Value* CursorPtr_;
};


////////////////////////////////////////////////////////////////////////////////

class TUnversionedRowValidatorBuilder
    : public TLogicalTypeValidatorBuilder
{
public:
    using TLogicalTypeValidatorBuilder::TLogicalTypeValidatorBuilder;

    void GenerateValidator(
        const TString& functionName,
        const TTableSchemaPtr& schema,
        TValidatorPersistentState* persistentState,
        bool typeAnyAcceptsAllValues,
        bool ignoreRequired,
        bool validateAnyIsValidYson);

private:
    class TUnversionedValueAccessor;

    void BuildValidateUnversionedValue(const TColumnSchema& schema, Value* unversionedValue);

    void BuildValidateColumnType(const TString& columnName, EValueType expectedType, Value* actualType);

    Value* IsAnyColumnCompatibleType(Value* valueType);

private:
    bool TypeAnyAcceptsAllValues_;
    bool IgnoreRequired_;
    bool ValidateAnyIsValidYson_;

    TValidatorPersistentState* PersistentState_;

    Value* UnversionedValuesPtr_;
    Value* PersistentStatePtr_;
};

////////////////////////////////////////////////////////////////////////////////

class TLogicalTypeValidatorBuilder::TFieldIdBuilder {
public:
    static Value* NewRootFieldId(TLogicalTypeValidatorBuilder& builder)
    {
        TFieldIdBuilder fieldIdBuilder(builder, nullptr);
        auto* nullptrValue = ConstantPointerNull::get(TTypeBuilder<TFieldId*>::Get(builder.Context));
        return fieldIdBuilder.CreateFieldIdWithConstSiblingIndex(
            /*parentPtr*/ nullptrValue,
            /*siblingIndex*/ 0);
    }

    TFieldIdBuilder(TLogicalTypeValidatorBuilder& builder, Value* fieldIdPtr)
        : Builder_(builder)
        , FieldIdPtr_(fieldIdPtr)
    { }

    Value* GetParent()
    {
        return Builder_.CreateLoad(
            TTypeBuilder<TFieldId>::TParent::Get(Builder_.Context),
            GetElementPtr(TTypeBuilder<TFieldId>::Fields::Parent));
    }

    Value* GetSiglingIndex()
    {
        return Builder_.CreateLoad(
            TTypeBuilder<TFieldId>::TSiblingIndex::Get(Builder_.Context),
            GetElementPtr(TTypeBuilder<TFieldId>::Fields::SiblingIndex));
    }

    Value* OptionalElement() const
    {
        return CreateFieldIdWithConstSiblingIndex(FieldIdPtr_, 0);
    }

    Value* ListElement() const
    {
        return CreateFieldIdWithConstSiblingIndex(FieldIdPtr_, 0);
    }

    Value* StructField(Value* index) const
    {
        return CreateFieldId(FieldIdPtr_, index);
    }

    Value* TupleElement(Value* index) const
    {
        return CreateFieldId(FieldIdPtr_, index);
    }

    Value* VariantStructField(Value* index) const
    {
        return CreateFieldId(FieldIdPtr_, index);
    }

    Value* VariantTupleElement(Value* index) const
    {
        return CreateFieldId(FieldIdPtr_, index);
    }

    Value* DictKey() const
    {
        return CreateFieldIdWithConstSiblingIndex(FieldIdPtr_, 0);
    }

    Value* DictValue() const
    {
        return CreateFieldIdWithConstSiblingIndex(FieldIdPtr_, 1);
    }

    Value* TaggedElement() const
    {
        return CreateFieldIdWithConstSiblingIndex(FieldIdPtr_, 0);
    }

private:
    Value* GetElementPtr(Value* fieldIdPtr, int indexInStruct) const
    {
        return Builder_.CreateStructGEP(
                TTypeBuilder<TFieldId>::Get(Builder_.Context),
                fieldIdPtr,
                indexInStruct);
    }

    Value* CreateFieldId(Value* parentPtr, Value* siblingIndex) const
    {
        YT_VERIFY(parentPtr->getType()->isPointerTy() && siblingIndex->getType()->isIntegerTy());
        auto* fieldIdPtr = Builder_.CreateAlloca(TTypeBuilder<TFieldId>::Get(Builder_.Context));
        Builder_.CreateStore(
            parentPtr,
            GetElementPtr(fieldIdPtr, TTypeBuilder<TFieldId>::Fields::Parent));
        Builder_.CreateStore(
            siblingIndex,
            GetElementPtr(fieldIdPtr, TTypeBuilder<TFieldId>::Fields::SiblingIndex));
        return fieldIdPtr;
    }

    Value* CreateFieldIdWithConstSiblingIndex(Value* parentPtr, ui64 siblingIndex) const
    {
        return CreateFieldId(
            parentPtr,
            Builder_.ConstantIntValue<ui64>(siblingIndex));
    }

    Value* GetElementPtr(int indexInStruct)
    {
        return GetElementPtr(FieldIdPtr_, indexInStruct);
    }

private:
    TLogicalTypeValidatorBuilder& Builder_;
    Value* FieldIdPtr_;
};

////////////////////////////////////////////////////////////////////////////////

class TLogicalTypeValidatorBuilder::TStringViewAccessor {
public:
    TStringViewAccessor(TIRBuilderBase& builder, Value* stringViewPtr)
        : Builder_(builder)
        , StringViewPtr_(stringViewPtr)
    { }

    Value* GetData()
    {
        return Builder_.CreateLoad(
            TTypeBuilder<TStringView>::TData::Get(Builder_.Context),
            GetDataPtr());
    }

    Value* GetLength()
    {
        return Builder_.CreateLoad(
            TTypeBuilder<TStringView>::TLength::Get(Builder_.Context),
            GetLengthPtr());
    }

    Value* GetDataPtr()
    {
        return GetElementPtr(TTypeBuilder<TStringView>::Fields::Data);
    }

    Value* GetLengthPtr()
    {
        return GetElementPtr(TTypeBuilder<TStringView>::Fields::Length);
    }

private:
    Value* GetElementPtr(int indexInStruct)
    {
        return Builder_.CreateStructGEP(
                TTypeBuilder<TStringView>::Get(Builder_.Context),
                StringViewPtr_,
                indexInStruct);
    }

private:
    TIRBuilderBase& Builder_;
    Value* StringViewPtr_;
};

////////////////////////////////////////////////////////////////////////////////

TLogicalTypeValidatorBuilder::TLogicalTypeValidatorBuilder(TCGModulePtr module)
    : TIRBuilderBase(std::move(module))
{ }

void TLogicalTypeValidatorBuilder::GenerateValidator(const TString& functionName, const TLogicalTypePtr& type)
{
    auto* function = Function::Create(
        TTypeBuilder<ILogicalTypeValidatorSignature>::Get(Context),
        Function::ExternalLinkage,
        functionName.c_str(),
        Module->GetModule());

    SetInsertPoint(CreateBB("impl", function));
    auto args = function->arg_begin();
    CursorPtr_ = ConvertToPointer(args);
    RootDescriptorPtr_ = ConvertToPointer(++args);
    YT_VERIFY(++args == function->arg_end());

    BuildValidateLogicalType(type, TFieldIdBuilder::NewRootFieldId(*this));

    CreateRetVoid();
}

void TLogicalTypeValidatorBuilder::BuildValidateLogicalType(const TLogicalTypePtr& type, Value* fieldIdPtr)
{
    switch (type->GetMetatype()) {
        case ELogicalMetatype::Simple:
            BuildValidateSimpleType(type->UncheckedAsSimpleTypeRef().GetElement(), fieldIdPtr);
            return;
        case ELogicalMetatype::Optional:
            BuildValidateOptionalType(type->UncheckedAsOptionalTypeRef(), fieldIdPtr);
            return;
        case ELogicalMetatype::List:
            BuildValidateListType(type->UncheckedAsListTypeRef(), fieldIdPtr);
            return;
        case ELogicalMetatype::Struct:
            BuildValidateStructType(type->UncheckedAsStructTypeRef(), fieldIdPtr);
            return;
        case ELogicalMetatype::Tuple:
            BuildValidateTupleType(type->UncheckedAsTupleTypeRef(), fieldIdPtr);
            return;
        case ELogicalMetatype::VariantStruct:
            BuildValidateVariantType(type->UncheckedAsVariantStructTypeRef(), fieldIdPtr);
            return;
        case ELogicalMetatype::VariantTuple:
            BuildValidateVariantType(type->UncheckedAsVariantTupleTypeRef(), fieldIdPtr);
            return;
        case ELogicalMetatype::Dict:
            BuildValidateDictType(type->UncheckedAsDictTypeRef(), fieldIdPtr);
            return;
        case ELogicalMetatype::Tagged:
            BuildValidateTaggedType(type->UncheckedAsTaggedTypeRef(), fieldIdPtr);
            return;
        case ELogicalMetatype::Decimal:
            BuildValidateDecimalType(type->UncheckedAsDecimalTypeRef(), fieldIdPtr);
            return;
    }
    YT_ABORT();
}

void TLogicalTypeValidatorBuilder::BuildValidateOptionalType(const TOptionalLogicalType& type, Value* fieldIdPtr)
{
    auto* currentTokenType = CreateCall(Module->GetRoutine("Cursor_GetCurrent_Type"), {CursorPtr_});
    TIfBuilder(*this, CreateICmpEQ(currentTokenType, ConstantEnumValue(EYsonItemType::EntityValue)))
        .Then([this](auto& builder) {
            builder.CreateCall(builder.Module->GetRoutine("Cursor_Next"), {CursorPtr_});
        })
        .Else([this, &type, &fieldIdPtr](auto& builder) {
            if (!type.IsElementNullable()) {
                TFieldIdBuilder fieldIdBuilder(builder, fieldIdPtr);
                BuildValidateLogicalType(type.GetElement(), fieldIdBuilder.OptionalElement());
                return;
            }

            BuildValidateYsonTokenType(EYsonItemType::BeginList, fieldIdPtr);
            builder.CreateCall(builder.Module->GetRoutine("Cursor_Next"), {CursorPtr_});

            BuildValidateYsonTokenTypeNotEqual(EYsonItemType::EndList, fieldIdPtr);

            TFieldIdBuilder fieldIdBuilder(builder, fieldIdPtr);
            BuildValidateLogicalType(type.GetElement(), fieldIdBuilder.OptionalElement());

            BuildValidateYsonTokenType(EYsonItemType::EndList, fieldIdPtr);
        });
}

void TLogicalTypeValidatorBuilder::BuildValidateListType(const TListLogicalType& type, Value* fieldIdPtr)
{
    BuildValidateYsonTokenType(EYsonItemType::BeginList, fieldIdPtr);
    CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});

    TFieldIdBuilder fieldIdBuilder(*this, fieldIdPtr);
    auto* elementFieldId = fieldIdBuilder.ListElement();

    TWhileBuilder(*this)
        .Condition([this](auto& builder) {
            auto* currentTokenType = builder.CreateCall(builder.Module->GetRoutine("Cursor_GetCurrent_Type"), {CursorPtr_});
            return builder.CreateICmpNE(currentTokenType, builder.ConstantEnumValue(EYsonItemType::EndList));
        })
        .Body([&type, &elementFieldId](auto& builder) {
            builder.BuildValidateLogicalType(type.GetElement(), elementFieldId);
        });
    CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});
}


void TLogicalTypeValidatorBuilder::BuildValidateStructType(const TStructLogicalType& type, Value* fieldIdPtr)
{
    BuildValidateYsonTokenType(EYsonItemType::BeginList, fieldIdPtr);
    CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});

    const auto& fields = type.GetFields();
    Value* isStructEnded = ConstantInt::getFalse(Context);
    for (size_t i = 0; i < fields.size(); ++i) {
        auto* currentTokenType = CreateCall(Module->GetRoutine("Cursor_GetCurrent_Type"), {CursorPtr_});
        isStructEnded = CreateOr(isStructEnded, CreateICmpEQ(currentTokenType, ConstantEnumValue(EYsonItemType::EndList)));

        const auto& field = fields[i];
        auto* isFieldRequired = ConstantInt::getBool(Context, field.Type->GetMetatype() != ELogicalMetatype::Optional);
        TIfBuilder(*this, CreateAnd(isStructEnded, isFieldRequired))
            .Then([this, &field, fieldIdPtr](auto& builder) {
                auto* fieldName = builder.CreateGlobalStringPtr(field.Name.Data());
                builder.CreateCall(
                    builder.Module->GetRoutine("ThrowMissingRequiredField"),
                    {fieldName, fieldIdPtr, RootDescriptorPtr_});
            })
            .Else([&field, i, isStructEnded, fieldIdPtr](auto& builder) {
                TFieldIdBuilder fieldIdBuilder(builder, fieldIdPtr);
                auto* fieldIndex = builder.template ConstantIntValue<ui64>(i);
                auto* structFieldIdPtr = fieldIdBuilder.StructField(fieldIndex);
                TIfBuilder(builder, isStructEnded)
                    .Else([&field, structFieldIdPtr](auto& builder) {
                        builder.BuildValidateLogicalType(field.Type, structFieldIdPtr);
                    });
            });
    }

    BuildValidateYsonTokenType(EYsonItemType::EndList, fieldIdPtr);
    CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});
}

void TLogicalTypeValidatorBuilder::BuildValidateTupleType(const TTupleLogicalType& type, Value* fieldIdPtr)
{
    BuildValidateYsonTokenType(EYsonItemType::BeginList, fieldIdPtr);
    CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});

    const auto& elements = type.GetElements();
    for (size_t i = 0; i < elements.size(); ++i) {
        TFieldIdBuilder fieldIdBuilder(*this, fieldIdPtr);
        auto* fieldIndex = ConstantIntValue<ui64>(i);
        auto* elementFieldId = fieldIdBuilder.TupleElement(fieldIndex);

        BuildValidateYsonTokenTypeNotEqual(EYsonItemType::EndList, elementFieldId);
        BuildValidateLogicalType(elements[i], elementFieldId);
    }

    BuildValidateYsonTokenType(EYsonItemType::EndList, fieldIdPtr);
    CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});
}

template <class TLogicalType>
    requires std::is_same_v<TLogicalType, TVariantTupleLogicalType> ||
             std::is_same_v<TLogicalType, TVariantStructLogicalType>
void TLogicalTypeValidatorBuilder::BuildValidateVariantType(const TLogicalType& type, Value* fieldIdPtr)
{
    BuildValidateYsonTokenType(EYsonItemType::BeginList, fieldIdPtr);
    CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});

    BuildValidateYsonTokenType(EYsonItemType::Int64Value, fieldIdPtr);
    auto* alternativeIndex = CreateCall(Module->GetRoutine("Cursor_UncheckedAsInt64"), {CursorPtr_});
    CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});

    const auto& elements = [&type] {
        if constexpr (std::is_same_v<TLogicalType, TVariantTupleLogicalType>) {
            return type.GetElements();
        } else {
            return type.GetFields();
        }
    }();

    {
        auto* zero = ConstantIntValue<ui64>(0);
        auto* alternativeCount = ConstantIntValue<ui64>(std::ssize(elements));

        auto* isLess = CreateICmpSLT(alternativeIndex, zero);
        auto* isGreater = CreateICmpSGE(alternativeIndex, alternativeCount);

        TIfBuilder(*this, CreateOr(isLess, isGreater))
            .Then([this, alternativeIndex, alternativeCount, fieldIdPtr](auto& builder) {
                builder.CreateCall(
                    builder.Module->GetRoutine("ThrowBadVariantAlternative"),
                    {alternativeIndex, alternativeCount, fieldIdPtr, RootDescriptorPtr_});
            });
    }

    TFieldIdBuilder fieldIdBuilder(*this, fieldIdPtr);
    {
        TSwitchBuilder switchBuilder(*this, alternativeIndex);
        for (size_t i = 0; i < elements.size(); ++i) {
            auto* index = ConstantIntValue<ui64>(i);
            switchBuilder.Case(index, [i, index, &elements, &fieldIdBuilder](auto& builder) {
                if constexpr (std::is_same_v<TLogicalType, TVariantTupleLogicalType>) {
                    builder.BuildValidateLogicalType(elements[i], fieldIdBuilder.VariantTupleElement(index));
                } else {
                    builder.BuildValidateLogicalType(elements[i].Type, fieldIdBuilder.VariantStructField(index));
                }
            });
        }
    }

    BuildValidateYsonTokenType(EYsonItemType::EndList, fieldIdPtr);
    CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});
}

void TLogicalTypeValidatorBuilder::BuildValidateDictType(const TDictLogicalType& type, Value* fieldIdPtr)
{
    BuildValidateYsonTokenType(EYsonItemType::BeginList, fieldIdPtr);
    CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});

    TFieldIdBuilder fieldIdBuilder(*this, fieldIdPtr);
    auto* keyFieldIdPtr = fieldIdBuilder.DictKey();
    auto* valueFieldIdPtr = fieldIdBuilder.DictValue();

    TWhileBuilder(*this)
        .Condition([this](auto& builder) {
            auto* currentTokenType = builder.CreateCall(builder.Module->GetRoutine("Cursor_GetCurrent_Type"), {CursorPtr_});
            return builder.CreateICmpNE(currentTokenType, builder.ConstantEnumValue(EYsonItemType::EndList));
        })
        .Body([this, &type, fieldIdPtr, keyFieldIdPtr, valueFieldIdPtr](auto& builder) {
            builder.BuildValidateYsonTokenType(EYsonItemType::BeginList, fieldIdPtr);
            builder.CreateCall(builder.Module->GetRoutine("Cursor_Next"), {CursorPtr_});

            builder.BuildValidateLogicalType(type.GetKey(), keyFieldIdPtr);
            builder.BuildValidateLogicalType(type.GetValue(), valueFieldIdPtr);

            builder.BuildValidateYsonTokenType(EYsonItemType::EndList, fieldIdPtr);
            builder.CreateCall(builder.Module->GetRoutine("Cursor_Next"), {CursorPtr_});
        });

    CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});
}

void TLogicalTypeValidatorBuilder::BuildValidateTaggedType(const TTaggedLogicalType& type, Value* fieldIdPtr)
{
    TFieldIdBuilder fieldIdBuilder(*this, fieldIdPtr);
    BuildValidateLogicalType(type.GetElement(), fieldIdBuilder.TaggedElement());
}

void TLogicalTypeValidatorBuilder::BuildValidateDecimalType(const TDecimalLogicalType& type, Value* fieldIdPtr)
{
    BuildValidateYsonTokenType(EYsonItemType::StringValue, fieldIdPtr);
    auto* binaryValue = CreateCall(Module->GetRoutine("Cursor_UncheckedAsStringView"), {CursorPtr_});
    auto* precisionValue = ConstantIntValue(type.GetPrecision());
    auto* scaleValue = ConstantIntValue(type.GetScale());
    CreateCall(
        Module->GetRoutine("ValidateDecimalValue"),
        {binaryValue, precisionValue, scaleValue, fieldIdPtr, RootDescriptorPtr_});
    CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});
}

void TLogicalTypeValidatorBuilder::BuildValidateSimpleType(ESimpleLogicalValueType type, Value* fieldIdPtr)
{
    Value* ysonItemTypeValue = CreateCall(Module->GetRoutine("Cursor_GetCurrent_Type"), {CursorPtr_});
    YT_VERIFY(ysonItemTypeValue->getType()->isIntegerTy());

    if (type == ESimpleLogicalValueType::Any) {
        TSwitchBuilder(*this, ysonItemTypeValue)
            .Case(
                ConstantEnumValue(EYsonItemType::EntityValue),
                [&fieldIdPtr](auto& builder) {
                    builder.CreateThrowParseError(
                        NTableClient::EErrorCode::SchemaViolation,
                        "unexpected entity value",
                        fieldIdPtr);
                })
            .Case(
                {
                    ConstantEnumValue(EYsonItemType::Int64Value),
                    ConstantEnumValue(EYsonItemType::Uint64Value),
                    ConstantEnumValue(EYsonItemType::BooleanValue),
                    ConstantEnumValue(EYsonItemType::DoubleValue),
                    ConstantEnumValue(EYsonItemType::StringValue),
                },
                [this](auto& builder) {
                    builder.CreateCall(builder.Module->GetRoutine("Cursor_Next"), {CursorPtr_});
                })
            .Case(
                ConstantEnumValue(EYsonItemType::BeginAttributes),
                [&fieldIdPtr](auto& builder) {
                    builder.CreateThrowParseError(
                        NTableClient::EErrorCode::SchemaViolation,
                        "unexpected top level attributes",
                        fieldIdPtr);
                })
            .Case(
                {
                    ConstantEnumValue(EYsonItemType::BeginList),
                    ConstantEnumValue(EYsonItemType::BeginMap),
                },
                [this](auto& builder) {
                    builder.CreateCall(builder.Module->GetRoutine("Cursor_SkipComplexValue"), {CursorPtr_});
                });
    } else {
        auto expectedYsonItemType = ExpectedYsonItemType(GetPhysicalType(type));
        auto* expectedYsonItemTypeValue = ConstantEnumValue(expectedYsonItemType);
        TIfBuilder(*this, CreateICmpNE(expectedYsonItemTypeValue, ysonItemTypeValue))
            .Then([this, &fieldIdPtr, &expectedYsonItemTypeValue](auto& builder) {
                builder.CreateCall(
                    builder.Module->GetRoutine("ThrowUnexpectedYsonToken"),
                    {CursorPtr_, expectedYsonItemTypeValue, fieldIdPtr, RootDescriptorPtr_});
            });

        switch (expectedYsonItemType) {
            case EYsonItemType::EntityValue:
                break;
#define CASE(x, cast_method)                                                                    \
                case x: {                                                                       \
                    auto *value = CreateCall(Module->GetRoutine(#cast_method), {CursorPtr_});   \
                    BuildValidateSimpleLogicalType<x>(type, value, fieldIdPtr);                 \
                    break;                                                                      \
                }
                CASE(EYsonItemType::BooleanValue, Cursor_UncheckedAsBoolean)
                CASE(EYsonItemType::Int64Value, Cursor_UncheckedAsInt64)
                CASE(EYsonItemType::Uint64Value, Cursor_UncheckedAsUint64)
                CASE(EYsonItemType::DoubleValue, Cursor_UncheckedAsDouble)
                CASE(EYsonItemType::StringValue, Cursor_UncheckedAsStringView)
#undef CASE
            default:
                Y_FAIL("unexpected EYsonItemType");
        }
        CreateCall(Module->GetRoutine("Cursor_Next"), {CursorPtr_});
    }
}

template <>
void TLogicalTypeValidatorBuilder::BuildValidateSimpleLogicalType<EYsonItemType::BooleanValue>(
    ESimpleLogicalValueType type,
    Value* value,
    Value* fieldIdPtr)
{
    Y_UNUSED(fieldIdPtr, value);

    if (type == ESimpleLogicalValueType::Boolean) {
        // do nothing
    } else {
        Y_FAIL("Bad logical type");
    }
}

template <>
void TLogicalTypeValidatorBuilder::BuildValidateSimpleLogicalType<EYsonItemType::Int64Value>(
    ESimpleLogicalValueType type,
    Value* value,
    Value* fieldIdPtr)
{
    if (
        type == ESimpleLogicalValueType::Int8 ||
        type == ESimpleLogicalValueType::Int16 ||
        type == ESimpleLogicalValueType::Int32 ||
        type == ESimpleLogicalValueType::Interval)
    {

        BuildValidateNumericRange<EYsonItemType::Int64Value>(
            value,
            GetLogicalTypeMin(*this, type),
            GetLogicalTypeMax(*this, type),
            fieldIdPtr);
    } else {
        YT_VERIFY(type == ESimpleLogicalValueType::Int64 && "Bad logical type");
        // Do nothing since Int64 doesn't require validation
    }
}

template <>
void TLogicalTypeValidatorBuilder::BuildValidateSimpleLogicalType<EYsonItemType::Uint64Value>(
    ESimpleLogicalValueType type,
    Value* value,
    Value* fieldIdPtr)
{
    if (
        type == ESimpleLogicalValueType::Uint8 ||
        type == ESimpleLogicalValueType::Uint16 ||
        type == ESimpleLogicalValueType::Uint32 ||
        type == ESimpleLogicalValueType::Date ||
        type == ESimpleLogicalValueType::Datetime ||
        type == ESimpleLogicalValueType::Timestamp)
    {
        BuildValidateNumericRange<EYsonItemType::Uint64Value>(
            value,
            GetLogicalTypeMin(*this, type),
            GetLogicalTypeMax(*this, type),
            fieldIdPtr);
    } else {
        YT_VERIFY(type == ESimpleLogicalValueType::Uint64 && "Bad logical type");
        // Do nothing since Uint64 doesn't require validation
    }
}


template <>
void TLogicalTypeValidatorBuilder::BuildValidateSimpleLogicalType<EYsonItemType::DoubleValue>(
    ESimpleLogicalValueType type,
    Value* value,
    Value* fieldIdPtr)
{
    if (type == ESimpleLogicalValueType::Float) {
        auto* positiveInf = ConstantFP::getInfinity(TTypeBuilder<double>::Get(Context), /*Negative*/ false);
        auto* negativeInf = ConstantFP::getInfinity(TTypeBuilder<double>::Get(Context), /*Negative*/ true);

        // Ordered Not Equal == True iff no nans and not equal.
        auto* notPositiveInf = CreateFCmpONE(value, positiveInf);
        auto* notNegativeInf = CreateFCmpONE(value, negativeInf);

        TIfBuilder(*this, CreateAnd(notPositiveInf, notNegativeInf))
            .Then([&value, &type, fieldIdPtr](TLogicalTypeValidatorBuilder& builder) {
                builder.BuildValidateNumericRange<EYsonItemType::DoubleValue>(
                    value,
                    GetLogicalTypeMin(builder, type),
                    GetLogicalTypeMax(builder, type),
                    fieldIdPtr);
            });
    } else if (type == ESimpleLogicalValueType::Double)  {
        // Do nothing.
    } else {
        YT_VERIFY(false && "Bad logical type");
    }
}

template <>
void TLogicalTypeValidatorBuilder::BuildValidateSimpleLogicalType<EYsonItemType::StringValue>(
    ESimpleLogicalValueType type,
    Value* value,
    Value* fieldIdPtr)
{
    auto* valuePtr = CreateAlloca(TTypeBuilder<TStringView>::Get(Context));
    CreateStore(value, valuePtr);
    auto stringAccessor = TStringViewAccessor(*this, valuePtr);
    if (type == ESimpleLogicalValueType::String) {
        // Do nothing.
    } else if (type == ESimpleLogicalValueType::Utf8) {
        auto* utf8Compat = CreateCall(Module->GetRoutine("UTF8Detect"), {stringAccessor.GetData(), stringAccessor.GetLength()});
        TIfBuilder(*this, CreateICmpEQ(utf8Compat, ConstantEnumValue(EUTF8Detect::NotUTF8)))
            .Then([fieldIdPtr](auto& builder) {
                builder.CreateThrowParseError(
                    NTableClient::EErrorCode::SchemaViolation,
                    "Not a valid utf8 string",
                    fieldIdPtr);
            });
    } else if (type == ESimpleLogicalValueType::Uuid) {
        auto uuidLength = ConstantIntValue<ui64>(16);
        TIfBuilder(*this, CreateICmpNE(stringAccessor.GetLength(), uuidLength))
            .Then([fieldIdPtr](auto& builder) {
                builder.CreateThrowParseError(
                    NTableClient::EErrorCode::SchemaViolation,
                    "Not a valid Uuid",
                    fieldIdPtr);
            });
    } else if (type == ESimpleLogicalValueType::Json) {
        CreateCall(Module->GetRoutine("ValidateJsonValue"), {value, fieldIdPtr, RootDescriptorPtr_});
    } else {
        YT_VERIFY(false && "Bad logical type");
    }
}

template <EYsonItemType itemType>
void TLogicalTypeValidatorBuilder::BuildValidateNumericRange(Value* value, Value* min, Value* max, Value* fieldIdPtr)
{
    CmpInst::Predicate lessPred;
    CmpInst::Predicate greaterPred;
    TStringBuf throwFunction;
    if constexpr (itemType == EYsonItemType::Int64Value) {
        lessPred = CmpInst::ICMP_SLT;
        greaterPred = CmpInst::ICMP_SGT;
        throwFunction = "ThrowOutOfRange_Int64";
    } else if constexpr (itemType == EYsonItemType::Uint64Value) {
        lessPred = CmpInst::ICMP_ULT;
        greaterPred = CmpInst::ICMP_UGT;
        throwFunction = "ThrowOutOfRange_Uint64";
    } else if constexpr (itemType == EYsonItemType::DoubleValue) {
        lessPred = CmpInst::FCMP_OLT;
        greaterPred = CmpInst::FCMP_OGT;
        throwFunction = "ThrowOutOfRange_Double";
    } else {
        static_assert(itemType == EYsonItemType::Int64Value, "unsupported yson item type");
    }

    YT_VERIFY(value->getType() == min->getType() && value->getType() == max->getType());
    auto* isLess = CreateCmp(lessPred, value, min);
    auto* isGreater = CreateCmp(greaterPred, value, max);
    TIfBuilder(*this, CreateOr(isLess, isGreater))
        .Then([this, value, min, max, throwFunction, fieldIdPtr](auto& builder) {
            builder.CreateCall(
                builder.Module->GetRoutine(throwFunction.data()),
                {value, min, max, fieldIdPtr, RootDescriptorPtr_});
        });
}

void TLogicalTypeValidatorBuilder::BuildValidateYsonTokenType(EYsonItemType type, Value* fieldIdPtr)
{
    BuildValidateYsonTokenTypeImpl(CmpInst::ICMP_NE, type, fieldIdPtr);
}

void TLogicalTypeValidatorBuilder::BuildValidateYsonTokenTypeNotEqual(EYsonItemType type, Value* fieldIdPtr)
{
    BuildValidateYsonTokenTypeImpl(CmpInst::ICMP_EQ, type, fieldIdPtr);
}

void TLogicalTypeValidatorBuilder::BuildValidateYsonTokenTypeImpl(
    CmpInst::Predicate failIfPredicate,
    EYsonItemType type,
    Value* fieldIdPtr)
{
    auto* tokenType = CreateCall(Module->GetRoutine("Cursor_GetCurrent_Type"), {CursorPtr_});
    auto* expectedTokenType = ConstantEnumValue(type);
    TIfBuilder(*this, CreateICmp(failIfPredicate, tokenType, expectedTokenType))
        .Then([this, &expectedTokenType, &fieldIdPtr](auto& builder) {
            builder.CreateCall(
                builder.Module->GetRoutine("ThrowUnexpectedYsonToken"),
                {CursorPtr_, expectedTokenType, fieldIdPtr, RootDescriptorPtr_});
        });
}

template <class TErrorCode>
void TLogicalTypeValidatorBuilder::CreateThrowParseError(TErrorCode errorCode, TStringBuf errorMessage, Value* fieldIdPtr)
{
    auto* errorCodeValue = ConstantEnumValue(errorCode);
    auto* errorMessageValue = CreateGlobalStringPtr(errorMessage.Data());
    CreateCall(Module->GetRoutine("ThrowParseError"), {errorCodeValue, errorMessageValue, fieldIdPtr, RootDescriptorPtr_});
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedRowValidatorBuilder::TUnversionedValueAccessor
{
public:
    TUnversionedValueAccessor(TIRBuilderBase& builder, Value* unversionedValuePtr)
        : Builder_(builder)
        , UnversionedValuePtr_(unversionedValuePtr)
    { }

    Value* GetType()
    {
        return Builder_.CreateLoad(
            TTypeBuilder<TUnversionedValue>::TType::Get(Builder_.Context),
            GetElementPtr(TTypeBuilder<TUnversionedValue>::Fields::Type));
    }

    Value* GetValue(EValueType valueType)
    {
        return GetData(GetUnversionedValueDataType(valueType));
    }

    Value* AsStringView()
    {
        auto* strData = GetData(GetUnversionedValueDataType(EValueType::String));
        auto* strLength = Builder_.CreateLoad(
            TTypeBuilder<TUnversionedValue>::TLength::Get(Builder_.Context),
            GetElementPtr(TTypeBuilder<TUnversionedValue>::Fields::Length));
        auto* strLengthLong = Builder_.CreateZExt(strLength, TTypeBuilder<TStringView>::TLength::Get(Builder_.Context));

        auto* valuePtr = Builder_.CreateAlloca(TTypeBuilder<TStringView>::Get(Builder_.Context));
        TStringViewAccessor accessor(Builder_, valuePtr);
        Builder_.CreateStore(strData, accessor.GetDataPtr());
        Builder_.CreateStore(strLengthLong, accessor.GetLengthPtr());
        return Builder_.CreateLoad(TTypeBuilder<TStringView>::Get(Builder_.Context), valuePtr);
    }

private:
    Value* GetData(Type* type)
    {
        return LoadElement(GetElementPtr(TTypeBuilder<TUnversionedValue>::Fields::Data), type);
    }

    Value* GetElementPtr(int indexInStruct)
    {
        return Builder_.CreateStructGEP(
                TTypeBuilder<TUnversionedValue>::Get(Builder_.Context),
                UnversionedValuePtr_,
                indexInStruct);
    }

    Value* LoadElement(Value* ptr, Type* type)
    {
        return Builder_.CreateLoad(type, Builder_.CreateBitCast(ptr, PointerType::getUnqual(type)));
    }

    Type* GetUnversionedValueDataType(EValueType type)
    {
        switch(type) {
            case EValueType::Int64:
                return TTypeBuilder<TUnversionedValueData>::TInt64::Get(Builder_.Context);
            case EValueType::Uint64:
                return TTypeBuilder<TUnversionedValueData>::TUint64::Get(Builder_.Context);
            case EValueType::Boolean:
                return TTypeBuilder<TUnversionedValueData>::TBoolean::Get(Builder_.Context);
            case EValueType::Double:
                return TTypeBuilder<TUnversionedValueData>::TDouble::Get(Builder_.Context);
            case EValueType::String:
                return TTypeBuilder<TUnversionedValueData>::TStringType::Get(Builder_.Context);
            default:
                YT_ABORT();
        }
    }

private:
    TIRBuilderBase& Builder_;
    Value* UnversionedValuePtr_;
};

////////////////////////////////////////////////////////////////////////////////

void TUnversionedRowValidatorBuilder::GenerateValidator(
    const TString& functionName,
    const TTableSchemaPtr& schema,
    TValidatorPersistentState* persistentState,
    bool typeAnyAcceptsAllValues,
    bool ignoreRequired,
    bool validateAnyIsValidYson)
{
    auto* function = Function::Create(
        TTypeBuilder<IRowValidatorImplSignature>::Get(Context),
        Function::ExternalLinkage,
        functionName.c_str(),
        Module->GetModule());

    TypeAnyAcceptsAllValues_ = typeAnyAcceptsAllValues;
    IgnoreRequired_ = ignoreRequired;
    ValidateAnyIsValidYson_ = validateAnyIsValidYson;
    PersistentState_ = persistentState;
    PersistentStatePtr_ = ConstantPointerValue(PersistentState_);
    CursorPtr_ = ConstantPointerValue(&*PersistentState_->Cursor);

    PersistentState_->Schema = schema;
    PersistentState_->FieldDescriptors.reserve(schema->GetColumnCount());

    SetInsertPoint(CreateBB("impl", function));
    auto args = function->arg_begin();
    UnversionedValuesPtr_ = ConvertToPointer(args);
    YT_VERIFY(++args == function->arg_end());

    const auto& columns = schema->Columns();
    for (size_t index = 0; index < columns.size(); ++index) {
        PersistentState_->FieldDescriptors.push_back(TComplexTypeFieldDescriptor(columns[index]));
        RootDescriptorPtr_ = ConstantPointerValue(&PersistentState_->FieldDescriptors.back());
        Value* unversionedValue = CreateConstInBoundsGEP1_64(
            TTypeBuilder<TUnversionedValue>::Get(Context),
            UnversionedValuesPtr_,
            index);
        BuildValidateUnversionedValue(columns[index], unversionedValue);
    }

    CreateRetVoid();

    // Verify that no reloactions have been performed, so all field descriptors pointers are valid.
    YT_VERIFY(PersistentState_->FieldDescriptors.capacity() == static_cast<size_t>(schema->GetColumnCount()));
}

void TUnversionedRowValidatorBuilder::BuildValidateUnversionedValue(
    const TColumnSchema& columnSchema,
    Value* unversionedValuePtr)
{
    // TODO(aleexfi): Improve exception messages.
    // Do smth like this https://github.com/ytsaurus/ytsaurus/blob/5e30734bd01a4cbfc17c3ab079b843984d13d325/yt/yt/client/table_client/unversioned_row.cpp#L1104-L1108.
    TUnversionedValueAccessor accessor(*this, unversionedValuePtr);
    auto* unversionedValueType = accessor.GetType();
    TIfBuilder(*this, CreateICmpEQ(unversionedValueType, ConstantEnumValue(EValueType::Null)))
        .Then([this, &columnSchema, unversionedValueType](auto& builder) {
            if (columnSchema.Required() && !IgnoreRequired_) {
                const auto& columnName = columnSchema.GetDiagnosticNameString();
                auto* columnNameValue = CreateGlobalStringPtr(columnName.data());
                builder.CreateCall(
                    builder.Module->GetRoutine("ThrowNullRequiredColumn"),
                    {columnNameValue, unversionedValueType});
            }
        })
        .Else([this, &columnSchema, &accessor, unversionedValueType](TUnversionedRowValidatorBuilder& builder) {
            auto v1Type = columnSchema.CastToV1Type();
            switch (v1Type) {
                case ESimpleLogicalValueType::Null:
                case ESimpleLogicalValueType::Void:
                    // This case should be handled before.
                    builder.BuildValidateColumnType(
                        columnSchema.GetDiagnosticNameString(),
                        EValueType::Null,
                        unversionedValueType);
                    return;
                case ESimpleLogicalValueType::Any:
                    if (columnSchema.IsOfV1Type()) {
                        if (!TypeAnyAcceptsAllValues_) {
                            builder.BuildValidateColumnType(
                                columnSchema.GetDiagnosticNameString(),
                                EValueType::Any,
                                unversionedValueType);
                        } else {
                            TIfBuilder(builder, IsAnyColumnCompatibleType(unversionedValueType))
                                .Then([&columnSchema, unversionedValueType](auto& builder) {
                                    const auto& columnName = columnSchema.GetDiagnosticNameString();
                                    auto* columnNameValue = builder.CreateGlobalStringPtr(columnName.data());
                                    builder.CreateCall(
                                        builder.Module->GetRoutine("ThrowBadColumnValue"),
                                        {columnNameValue, unversionedValueType, builder.ConstantEnumValue(EValueType::Any)});
                                });
                        }
                        if (ValidateAnyIsValidYson_) {
                            auto* isAny = builder.CreateICmpEQ(unversionedValueType, ConstantEnumValue(EValueType::Any));
                            auto* isComposite = builder.CreateICmpEQ(unversionedValueType, ConstantEnumValue(EValueType::Composite));
                            TIfBuilder(builder, CreateOr(isAny, isComposite))
                                .Then([&accessor](auto& builder) {
                                    builder.CreateCall(builder.Module->GetRoutine("ValidateYsonValue"), accessor.AsStringView());
                                });
                        }
                    } else {
                        builder.BuildValidateColumnType(
                            columnSchema.GetDiagnosticNameString(),
                            EValueType::Composite,
                            unversionedValueType);
                        builder.CreateCall(
                            builder.Module->GetRoutine("InitYsonParser"),
                            {PersistentStatePtr_, accessor.AsStringView()});
                        builder.BuildValidateLogicalType(columnSchema.LogicalType(), TFieldIdBuilder::NewRootFieldId(builder));
                    }
                    return;
                case ESimpleLogicalValueType::String:
                    if (columnSchema.IsOfV1Type()) {
                        builder.BuildValidateSimpleLogicalType<EYsonItemType::StringValue>(
                            ESimpleLogicalValueType::String,
                            accessor.AsStringView(),
                            TFieldIdBuilder::NewRootFieldId(builder));
                    } else {
                        builder.BuildValidateColumnType(
                            columnSchema.GetDiagnosticNameString(),
                            EValueType::String,
                            unversionedValueType);
                        auto type = UnwrapTaggedAndOptional(columnSchema.LogicalType());
                        YT_VERIFY(type->GetMetatype() == ELogicalMetatype::Decimal);

                        auto* precision = builder.ConstantIntValue<int>(type->UncheckedAsDecimalTypeRef().GetPrecision());
                        auto* scale = builder.ConstantIntValue<int>(type->UncheckedAsDecimalTypeRef().GetScale());
                        builder.CreateCall(builder.Module->GetRoutine("ValidateDecimalValue"),
                            {
                                accessor.AsStringView(),
                                precision,
                                scale,
                                TFieldIdBuilder::NewRootFieldId(builder),
                                RootDescriptorPtr_});
                    }
                    return;
#define CASE(x)                                                                                 \
                case x: {                                                                       \
                    constexpr auto physicalType = GetPhysicalType(x);                           \
                    constexpr auto ysonItemType = ExpectedYsonItemType(physicalType);           \
                    builder.BuildValidateColumnType(                                            \
                        columnSchema.GetDiagnosticNameString(),                                 \
                        physicalType,                                                           \
                        unversionedValueType);                                                  \
                    builder.BuildValidateSimpleLogicalType<ysonItemType>(                       \
                        x, accessor.GetValue(physicalType),                                     \
                        TFieldIdBuilder::NewRootFieldId(builder));                              \
                    return;                                                                     \
                }

                CASE(ESimpleLogicalValueType::Int64)
                CASE(ESimpleLogicalValueType::Uint64)
                CASE(ESimpleLogicalValueType::Double)
                CASE(ESimpleLogicalValueType::Boolean)

                CASE(ESimpleLogicalValueType::Float)

                CASE(ESimpleLogicalValueType::Int8)
                CASE(ESimpleLogicalValueType::Int16)
                CASE(ESimpleLogicalValueType::Int32)

                CASE(ESimpleLogicalValueType::Uint8)
                CASE(ESimpleLogicalValueType::Uint16)
                CASE(ESimpleLogicalValueType::Uint32)

                CASE(ESimpleLogicalValueType::Utf8)
                CASE(ESimpleLogicalValueType::Date)
                CASE(ESimpleLogicalValueType::Datetime)
                CASE(ESimpleLogicalValueType::Timestamp)
                CASE(ESimpleLogicalValueType::Interval)
                CASE(ESimpleLogicalValueType::Json)
                CASE(ESimpleLogicalValueType::Uuid)
                default:
                    YT_ABORT();
            }
#undef CASE
        });
}

void TUnversionedRowValidatorBuilder::BuildValidateColumnType(const TString& columnName, EValueType expectedType, Value* actualType)
{
    auto* expectedTypeValue = ConstantEnumValue(expectedType);
    TIfBuilder(*this, CreateICmpNE(actualType, expectedTypeValue))
        .Then([columnName, expectedTypeValue, actualType](auto& builder) {
            auto columnNameValue = builder.CreateGlobalStringPtr(columnName.data());
            builder.CreateCall(
                builder.Module->GetRoutine("ThrowBadColumnValue"),
                {columnNameValue, expectedTypeValue, actualType});
        });
}

Value* TUnversionedRowValidatorBuilder::IsAnyColumnCompatibleType(Value* valueType)
{
    return CreateOr({
        CreateICmpEQ(valueType, ConstantEnumValue(EValueType::Null)),
        CreateICmpEQ(valueType, ConstantEnumValue(EValueType::Int64)),
        CreateICmpEQ(valueType, ConstantEnumValue(EValueType::Uint64)),
        CreateICmpEQ(valueType, ConstantEnumValue(EValueType::Double)),
        CreateICmpEQ(valueType, ConstantEnumValue(EValueType::Boolean)),
        CreateICmpEQ(valueType, ConstantEnumValue(EValueType::String)),
        CreateICmpEQ(valueType, ConstantEnumValue(EValueType::Any)),
        CreateICmpEQ(valueType, ConstantEnumValue(EValueType::Composite)),
    });
}

////////////////////////////////////////////////////////////////////////////////

class TRowValidatorProvider
    : public IRowValidatorProvider
{
    TCallback<ILogicalTypeValidatorSignature> Get(TLogicalTypePtr logicalType) override
    {
        // TODO(aleexfi): Maybe cache compiled functinos.
        auto module = TCGModule::Create(GetValidatorRoutineRegistry());
        auto builder = TLogicalTypeValidatorBuilder(module);

        const TString functionName = "ValidateLogicalType";

        builder.GenerateValidator(functionName, logicalType);

        module->ExportSymbol(functionName);

        return module->GetCompiledFunction<ILogicalTypeValidatorSignature>(functionName);
    };

    TCallback<IRowValidatorSignature> Get(
        const TTableSchemaPtr& schema,
        bool typeAnyAcceptsAllValues,
        bool ignoreRequired,
        bool validateAnyIsValidYson) override
    {
        auto module = TCGModule::Create(GetValidatorRoutineRegistry());
        auto builder = TUnversionedRowValidatorBuilder(module);

        const TString functionName = "ValidateRow";

        auto persistentState = std::make_unique<TValidatorPersistentState>();
        builder.GenerateValidator(
            functionName,
            schema,
            persistentState.get(),
            typeAnyAcceptsAllValues,
            ignoreRequired,
            validateAnyIsValidYson);

        module->ExportSymbol(functionName);
        auto callback = module->GetCompiledFunction<IRowValidatorImplSignature>(functionName);
        return BIND([callback = std::move(callback), state = std::move(persistentState)](const TUnversionedRow* row) {
            THROW_ERROR_EXCEPTION_IF(state->Schema->GetColumnCount() != static_cast<i64>(row->GetCount()),
                NTableClient::EErrorCode::SchemaViolation,
                "Unversioned row consists of %v columns, but %v expected",
                row->GetCount(),
                state->Schema->GetColumnCount());
            return callback(row->Begin());
        });
    };
};

IRowValidatorProviderPtr CreateRowValidatorProvider()
{
    return New<TRowValidatorProvider>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
