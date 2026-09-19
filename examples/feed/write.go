package feed

import (
	"context"
	"errors"

	"github.com/21strive/redifu"
)

// CreatePost writes the post and puts it into one feed.
//
// AddItem stores the post if Base does not hold it yet and always refreshes its TTL,
// then places it into the index. ErrNotIngested means it did the first part but not
// the second — that feed has never been seeded, or the post sorts outside the window
// the feed currently holds. It is an outcome, not a failure: the post shows up when
// the feed is next seeded.
func (s *Store) CreatePost(ctx context.Context, post *Post, feedUserRandIds []string) error {
	if err := s.PostBase.Set(ctx, post); err != nil {
		return err
	}

	for _, userRandId := range feedUserRandIds {
		err := s.PostFeed.AddItem(ctx, post, userRandId)
		if err != nil && !errors.Is(err, redifu.ErrNotIngested) {
			return err
		}
	}

	return nil
}

// FanOutPost is the same thing for a large audience, in one pipeline.
//
// Each AddItem is a single Lua script: the window check, the marker updates and the
// write happen inside Redis as one atomic command. Ten thousand followers cost ten
// thousand enqueued commands over a handful of round-trips.
//
// The trade: on this path you cannot see which targets reported ErrNotIngested, since
// that is only known after Exec.
func (s *Store) FanOutPost(ctx context.Context, post *Post, followerRandIds []string) error {
	pipe := s.redis.Pipeline()

	if err := s.PostBase.WithPipeline(pipe).Set(ctx, post); err != nil {
		return err
	}
	for _, followerRandId := range followerRandIds {
		if err := s.PostFeed.WithPipeline(pipe).AddItem(ctx, post, followerRandId); err != nil {
			return err
		}
	}

	_, err := pipe.Exec(ctx)
	return err
}

// UpdatePost changes the post's contents. AddItem would not do this — it only indexes.
func (s *Store) UpdatePost(ctx context.Context, post *Post) error {
	return s.PostBase.Set(ctx, post)
}

// RenameOrganisation is the payoff of the whole arrangement.
//
// One write, two levels down the chain. Every post by every account in that
// organisation reflects it on the next read — no feed purged, no page reseeded,
// nothing invalidated.
func (s *Store) RenameOrganisation(ctx context.Context, randId, name string) error {
	organisation, err := s.OrganisationBase.Get(ctx, randId)
	if err != nil {
		return err
	}

	organisation.Name = name
	return s.OrganisationBase.Set(ctx, organisation)
}

// DeletePost removes the entity and the index member. Deleting the entity is always
// explicit: RemoveItem only detaches the post from that one feed, because it may well
// be a member of others.
//
// RemoveItem also clears that feed's first-page and last-page markers. It has to:
// "nothing exists before this page" stopped being true the moment an item left. So the
// next read of an affected feed reports RequiresSeeding and rebuilds from the
// database. Budget for that when deleting something with a wide fan-out.
func (s *Store) DeletePost(ctx context.Context, post *Post, feedUserRandIds []string) error {
	pipe := s.redis.Pipeline()

	for _, userRandId := range feedUserRandIds {
		if err := s.PostFeed.WithPipeline(pipe).RemoveItem(ctx, post, userRandId); err != nil {
			return err
		}
	}
	if err := s.PostBase.WithPipeline(pipe).Del(ctx, post); err != nil {
		return err
	}

	_, err := pipe.Exec(ctx)
	return err
}
