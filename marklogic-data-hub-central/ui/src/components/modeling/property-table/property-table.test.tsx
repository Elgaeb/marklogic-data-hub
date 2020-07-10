import React from 'react';
import { render, screen, fireEvent, wait, getByTestId } from '@testing-library/react';
import userEvent from "@testing-library/user-event";
import PropertyTable from './property-table';

import { ModelingContext } from '../../../util/modeling-context';
import { ModelingTooltips } from '../../../config/tooltips.config';
import { propertyTableEntities } from '../../../assets/mock-data/modeling';
import { entityNamesArray } from '../../../assets/mock-data/modeling-context-mock';


describe('Entity Modeling Property Table Component', () => {
  window.scrollTo = jest.fn()

  test('Property Table renders an Entity with no properties, no writer role', () => {
    let entityName = 'NewEntity';
    let definitions = { NewEntity : { properties: {} } };
    const { getByText, getByLabelText } =  render(
      <PropertyTable
        canReadEntityModel={true}
        canWriteEntityModel={false}
        entityName={entityName} 
        definitions={definitions}
      />
    )
    

    expect(getByText('Add Property')).toBeInTheDocument();
    expect(getByText('Property Name')).toBeInTheDocument();
    expect(getByText('Multiple')).toBeInTheDocument();
    expect(getByText('Sort')).toBeInTheDocument();
    expect(getByText('Wildcard Search')).toBeInTheDocument();
    expect(getByLabelText('NewEntity-add-property')).toBeDisabled();
  });

  test('Property Table renders with basic datatypes, with writer role & hover text shows', async () => {
    let entityName = propertyTableEntities[0].entityName;
    let definitions = propertyTableEntities[0].model.definitions;
    const { getByText, getByTestId, getByLabelText, debug } =  render(
      <PropertyTable 
        canReadEntityModel={true}
        canWriteEntityModel={true}
        entityName={entityName} 
        definitions={definitions}
      />
    )

    fireEvent.mouseOver(getByLabelText('identifier-header'));
    await wait (() => expect(screen.getByText(ModelingTooltips.identifier)).toBeInTheDocument());

    fireEvent.mouseOver(getByLabelText('multiple-header'));
    await wait (() => expect(screen.getByText(ModelingTooltips.multiple)).toBeInTheDocument());

    fireEvent.mouseOver(getByLabelText('sort-header'));
    await wait (() => expect(screen.getByText(ModelingTooltips.sort)).toBeInTheDocument());

    fireEvent.mouseOver(getByLabelText('facet-header'));
    await wait (() => expect(screen.getByText(ModelingTooltips.facet)).toBeInTheDocument());

    fireEvent.mouseOver(getByLabelText('wildcard-header'));
    await wait (() => expect(screen.getByText(ModelingTooltips.wildcard)).toBeInTheDocument());

    fireEvent.mouseOver(getByLabelText('pii-header'));
    await wait (() => expect(screen.getByText(ModelingTooltips.pii)).toBeInTheDocument());

    expect(getByTestId('identifier-concept_name')).toBeInTheDocument();
    expect(getByTestId('multiple-synonyms')).toBeInTheDocument();
    expect(getByTestId('pii-source_concept_code')).toBeInTheDocument();
    expect(getByTestId('wildcard-vocabulary')).toBeInTheDocument();

    expect(getByText('invalid_reason')).toBeInTheDocument();
    expect(getByText('vocabulary')).toBeInTheDocument();
    expect(getByText('synonyms')).toBeInTheDocument();
    expect(getByText('unsignedLong')).toBeInTheDocument();

    userEvent.click(getByLabelText('Concept-add-property'));
    expect(getByText('Entity Type:')).toBeInTheDocument();
    expect(getByText('Add')).toBeInTheDocument();
    expect(getByText('Cancel')).toBeInTheDocument();
  });

  test('Property Table renders with structured and external datatypes, no writer role', () => {
    let entityName = propertyTableEntities[2].entityName;
    let definitions = propertyTableEntities[2].model.definitions;
    const { getByText, getByTestId, getAllByText, getAllByTestId, getAllByRole, getByLabelText, queryByTestId } =  render(
      <PropertyTable 
        canReadEntityModel={true}
        canWriteEntityModel={false}
        entityName={entityName} 
        definitions={definitions}
      />
    )

    expect(getByLabelText('Customer-add-property')).toBeDisabled();
    expect(getByTestId('identifier-customerId')).toBeInTheDocument();
    expect(getByTestId('multiple-orders')).toBeInTheDocument();
    expect(getAllByTestId('add-struct-Address')).toHaveLength(2); 
    expect(queryByTestId('customerId-span')).toBeNull();

    expect(getByText('Order')).toBeInTheDocument();
    expect(getByText('integer')).toBeInTheDocument();
    expect(getByText('birthDate')).toBeInTheDocument();
    expect(getByText('billing')).toBeInTheDocument();
    expect(getByText('shipping')).toBeInTheDocument();

    expect(getAllByText('string')).toHaveLength(3);
    expect(getAllByText('Address')).toHaveLength(2);
    
    // Table expansion shipping property -> Address Structure type
    userEvent.click(getAllByRole('img')[0]);

    expect(getByTestId('add-struct-Zip')).toBeInTheDocument();
    expect(getAllByText(/zip/i)).toHaveLength(2);
    expect(getAllByText('street')).toHaveLength(1);
    expect(getAllByText('state')).toHaveLength(1);

    // Table expansion for shipping property -> Zip structure type
    userEvent.click(getAllByRole('img')[1]);

    expect(getByText('fiveDigit')).toBeInTheDocument();
    expect(getByText('plusFour')).toBeInTheDocument();

    // Table expansion for billing property, Address structure type
    userEvent.click(getAllByRole('img')[2]);

    expect(getAllByTestId('add-struct-Zip')).toHaveLength(2); 
    expect(getAllByText(/zip/i)).toHaveLength(4);
    expect(getAllByText('street')).toHaveLength(2);
    expect(getAllByText('state')).toHaveLength(2);

    // Table expansion for billing property -> Zip structure type
    userEvent.click(getAllByRole('img')[3]);

    expect(getAllByText('fiveDigit')).toHaveLength(2);
    expect(getAllByText('plusFour')).toHaveLength(2);
    expect(getAllByText('string')).toHaveLength(13);
  });

  test('can add a Property to the table and then edit it', async () => {
    let entityName = propertyTableEntities[0].entityName;
    let definitions = propertyTableEntities[0].model.definitions;
    const { getByText, getByTestId, getByLabelText, debug } =  render(
      <PropertyTable 
        canReadEntityModel={true}
        canWriteEntityModel={true}
        entityName={entityName} 
        definitions={definitions}
      />
    )

    userEvent.click(screen.getByLabelText('Concept-add-property'));

    await userEvent.type(screen.getByLabelText('input-name'), 'conceptDate');
    userEvent.click(screen.getByPlaceholderText('Select the property type'));
    userEvent.click(screen.getByText('dateTime'));

    fireEvent.submit(screen.getByLabelText('input-name'));

    expect(getByTestId('conceptDate-span')).toBeInTheDocument();

    userEvent.click(screen.getByTestId('conceptDate-span'));

    userEvent.clear(screen.getByLabelText('input-name'));
    await userEvent.type(screen.getByLabelText('input-name'), 'conception');
    
    fireEvent.submit(screen.getByLabelText('input-name'));
    expect(getByTestId('conception-span')).toBeInTheDocument();
  });

  test('can add a new structured type property to the table and then edit it', async () => {
    let entityName = propertyTableEntities[0].entityName;
    let definitions = propertyTableEntities[0].model.definitions;
    const { getByText, getByTestId } =  render(
      <PropertyTable 
        canReadEntityModel={true}
        canWriteEntityModel={true}
        entityName={entityName} 
        definitions={definitions}
      />
    )

    userEvent.click(screen.getByLabelText('Concept-add-property'));

    await userEvent.type(screen.getByLabelText('input-name'), 'newStructure');
    userEvent.click(screen.getByPlaceholderText('Select the property type'));
    userEvent.click(screen.getByText('Structured'));
    userEvent.click(screen.getByText('New Property Type'));

    expect(screen.getByText('Add New Structured Property Type')).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText('structured-input-name'), 'Product');
    fireEvent.submit(screen.getByLabelText('structured-input-name'));

    fireEvent.submit(screen.getByLabelText('input-name'));

    expect(getByTestId('newStructure-span')).toBeInTheDocument();

    userEvent.click(screen.getByTestId('newStructure-span'));
    userEvent.clear(screen.getByLabelText('input-name'));
    await userEvent.type(screen.getByLabelText('input-name'), 'basicName');
    userEvent.click(screen.getByLabelText('type-dropdown'));
    userEvent.click(screen.getByText('More date types'));
    userEvent.click(screen.getByText('dayTimeDuration'));
    
    fireEvent.submit(screen.getByLabelText('input-name'));
    expect(getByTestId('conception-span')).toBeInTheDocument();
  });

  test('can edit a property and change the type from basic to relationship', async () => {
    let entityName = propertyTableEntities[2].entityName;
    let definitions = propertyTableEntities[2].model.definitions;
    const { getByText, getByTestId, queryByTestId, getAllByTestId } =  render(
      <ModelingContext.Provider value={entityNamesArray}>
        <PropertyTable 
          canReadEntityModel={true}
          canWriteEntityModel={true}
          entityName={entityName} 
          definitions={definitions}
        />
      </ModelingContext.Provider>
    )

    expect(getByTestId('identifier-customerId')).toBeInTheDocument();
    expect(getByTestId('multiple-orders')).toBeInTheDocument();
    expect(getAllByTestId('add-struct-Address')).toHaveLength(2); 

    userEvent.click(getByTestId('nicknames-span'));
    userEvent.clear(screen.getByLabelText('input-name'));

    await userEvent.type(screen.getByLabelText('input-name'), 'altName');

    const multipleRadio = screen.getByLabelText('multiple-yes');
    fireEvent.change(multipleRadio, { target: { value: "yes" } });
    expect(multipleRadio['value']).toBe('yes');

    const piiRadio = screen.getByLabelText('pii-yes')
    fireEvent.change(piiRadio, { target: { value: "yes" } });
    expect(piiRadio['value']).toBe('yes');

    const wildcardCheckbox = screen.getByLabelText('Wildcard Search')
    fireEvent.change(wildcardCheckbox, { target: { checked: true } });
    expect(wildcardCheckbox).toBeChecked();

    fireEvent.submit(screen.getByLabelText('input-name'));
    expect(getByTestId('altName-span')).toBeInTheDocument();
    userEvent.click(screen.getByTestId('altName-span'));

    userEvent.clear(screen.getByLabelText('input-name'));
    await userEvent.type(screen.getByLabelText('input-name'), 'orderRelationship');
    userEvent.click(screen.getByLabelText('type-dropdown'));
    userEvent.click(screen.getByText('Relationship'));
    userEvent.click(screen.getAllByText('Order')[0]);
    fireEvent.submit(screen.getByLabelText('input-name'));

    expect(getByTestId('orderRelationship-span')).toBeInTheDocument();
    userEvent.click(screen.getByTestId('orderRelationship-span'));

    userEvent.clear(screen.getByLabelText('input-name'));
    await userEvent.type(screen.getByLabelText('input-name'), 'basicID');
    userEvent.click(screen.getByLabelText('type-dropdown'));
    userEvent.click(screen.getAllByText('integer')[0]);
    fireEvent.submit(screen.getByLabelText('input-name'));

    expect(getByTestId('basicID-span')).toBeInTheDocument();
  });
});
