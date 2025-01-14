(:
  Copyright (c) 2021 MarkLogic Corporation

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
:)
xquery version "1.0-ml";

(: This library is for operations for mapping that are more dificult to accomplish in JavaScript :)
module namespace xquery-lib = "http://marklogic.com/mapping/es/xquery";

import module namespace inst="http://marklogic.com/entity-services-instance" at "/MarkLogic/entity-services/entity-services-instance.xqy";
import module namespace es="http://marklogic.com/entity-services" at "/MarkLogic/entity-services/entity-services.xqy";
import module namespace json = "http://marklogic.com/xdmp/json" at "/MarkLogic/json/json.xqy";

declare function document-with-nodes($nodes as node()*) {
  document {
    $nodes
  }
};

(:
Copy of this function from ML 10.0-3. The only change is the addition of user-params.
Note that "parms" in the code below should really be "options", which would then match the signature of xslt-eval.

I opened bug 54632 to improve the ES functions so that a map of params will be accepted. Then we can get rid of this
hack.
:)
declare function data-hub-map-to-canonical(
  $source-instance as node(),
  $mapping-uri as xs:string,
  $user-params as map:map?,
  $options as map:map
) as node()
{
  let $target-entity-name := $options=>map:get("entity")
  let $format := $options=>map:get("format")
  let $format :=
    if (empty($format)) then
      typeswitch ($source-instance)
        case document-node() return
          if ($source-instance/element()) then "xml"
          else "json"
        case element() return "xml"
        default return "json"
    else $format
  let $input :=
    typeswitch ($source-instance)
      case document-node() return $source-instance
      case element() return document { $source-instance }
      default return document { $source-instance }
  let $parms :=
    if (empty($target-entity-name)) then ()
    else map:map()=>map:with("template", $target-entity-name)
  let $results :=
    xdmp:xslt-invoke($mapping-uri||".xslt", $input, $user-params, $parms)
  return
    if ($format="xml")
    then inst:canonical-xml($results)
    else(
      let $element := $results/element()
      return
      (: This change is necessitated by DHFPROD-6219. In case the entity doesn't have strucutred properties, it
         returns empty json object. If it has structured properties, the empty structured properties are removed.
         inst:canonical-json() method introduces the '$ref'.  :)
      if (empty($element/*)) then
        document{json:object()=>map:with(string(fn:node-name($element)), json:object())}
      else
        inst:canonical-json(xquery-lib:remove-empty-structured-properties($element))
    )
};


declare function remove-empty-structured-properties($element as item()*) as document-node() {
  document{
    element {fn:node-name($element)} {
      $element/namespace::node(),
      $element/@*,
      for $child in $element/element()
      where not($child/element() instance of element() and empty($child/element()/element()))
      return $child
    }
  }
};

declare %private variable $fetch := '
  declare variable $uri as xs:string external;
  fn:doc($uri)
';
declare %private variable $put := '
  declare variable $uri as xs:string external;
  declare variable $node as node() external;
  declare variable $collection as xs:string external;
  declare variable $xslt-uri as xs:string := $uri||".xslt";
  declare variable $options :=
    <options xmlns="xdmp:document-insert">
      <permissions>{xdmp:document-get-permissions($uri,"elements")}</permissions>
      <collections>{for $c in xdmp:document-get-collections($uri) return <collection>{$c}</collection>,
        <collection>{$collection}</collection>
      }</collections>
    </options>;

  xdmp:document-insert($xslt-uri, $node, $options)
';

(: xqueryLib.functionMetadataPut should be used instead of the OOTB es.functionMetadataPut in order to allow
sequence to be passed to javascript mapping functions. This function makes use of
/data-hub/5/mapping/entity-services/function-metadata.xsl  (which is the modified version of
/MarkLogic/entity-services/function-metadata.xsl) to resolve https://project.marklogic.com/jira/browse/DHFPROD-5850
:)
declare function function-metadata-put(
  $uri as xs:string
) as empty-sequence()
{
  let $source :=
    xdmp:eval(
      $fetch,
      map:map()=>map:with("uri",$uri),
      map:map()=>map:with("database",xdmp:modules-database()))
  let $compiled := xquery-lib:function-metadata-compile($source)
  return
    xdmp:eval($put,
      map:map()=>map:with("uri",$uri)=>
      map:with("node",$compiled)=>
      map:with("collection",$es:FUNCTIONDEF_COLLECTION)
      ,
      map:map()=>map:with("database",xdmp:modules-database()))
};

declare function function-metadata-compile(
  $function-metadata as node()
) as node()
{
  let $input := es:function-metadata-validate($function-metadata)
  return
    xdmp:xslt-invoke("/data-hub/5/mapping/entity-services/function-metadata.xsl", $input)
};

