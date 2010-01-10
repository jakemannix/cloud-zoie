package proj.zoie.example.service;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.FacetHandlerFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BoboIndexReaderDecorator implements IndexReaderDecorator<BoboIndexReader> {

  protected List<FacetHandlerFactory> facetFactories;

  public BoboIndexReaderDecorator(List<FacetHandlerFactory> facetFactories) {
    this.facetFactories = facetFactories;
  }

  public static List<FacetHandler> buildFacetHandlers(List<FacetHandlerFactory> facetHandlerFactories) {
	  ArrayList<FacetHandler> retList = null;

	  if (facetHandlerFactories != null)
	  {
		  retList = new ArrayList<FacetHandler>(facetHandlerFactories.size());
		  for (FacetHandlerFactory factory : facetHandlerFactories)
		  {
			  FacetHandler handler = factory.newInstance();
			  retList.add(handler);
		  }
	  }
	  return retList;
  }
  @Override
  public BoboIndexReader decorate(ZoieIndexReader<BoboIndexReader> reader) throws IOException {
    return BoboIndexReader.getInstance(reader, buildFacetHandlers(facetFactories));
  }

  @Override
  public BoboIndexReader redecorate(BoboIndexReader decorated, ZoieIndexReader<BoboIndexReader> copy) throws IOException {
    return decorate(copy);
  }
}
